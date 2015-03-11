/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
	region = "us-east-1";
	console.log("Using default region " + region);
}

var aws = require('aws-sdk');
var s3 = new aws.S3({
	apiVersion : '2006-03-01',
	region : region
});
var dynamoDB = new aws.DynamoDB({
	apiVersion : '2012-08-10',
	region : region
});
var sns = new aws.SNS({
	apiVersion : '2010-03-31',
	region : region
});
var jdbc = new (require('jdbc'));
var java = require('java');
require('./constants');
var kmsCrypto = require('./kmsCrypto');
var common = require('./common');
var async = require('async');
var uuid = require('node-uuid');

// main function for AWS Lambda
exports.handler = function(event, context) {
	/** runtime functions * */

	/* callback run when we find a configuration for load in Dynamo DB */
	var foundConfig = function(s3Info, err, data) {
		if (err) {
			console.log(err);
			var msg = 'Error getting Redshift Configuration for ' + s3Info.prefix + ' from Dynamo DB ';
			console.log(msg);
			context.done(error, msg);
		}

		if (!data || !data.Item) {
			// finish with no exception - where this file sits in the S3
			// structure is not configured for redshift loads
			console.log("unable to load configuration for " + s3Info.prefix);

			context.done(null, null);
		} else {
			console.log("Found Redshift Load Configuration for " + s3Info.prefix);

			var config = data.Item;
			var thisBatchId = config.currentBatch.S;

			if (config.filenameFilterRegex) {
				if (s3Info.key.match(config.filenameFilterRegex.S)) {
					checkFileProcessed(config, thisBatchId, s3Info);
				} else {
					console.log('Object ' + s3Info.key + ' excluded by filename filter \'' + config.filenameFilterRegex.S + '\'');

					// scan the current batch to decide if it needs to be
					// flushed due to batch timeout
					processPendingBatch(config, thisBatchId, s3Info);
				}
			} else {
				// no filter, so we'll load the data
				checkFileProcessed(config, thisBatchId, s3Info);
			}
		}
	};

	/*
	 * function to add a file to the pending batch set and then call the success
	 * callback
	 */
	var checkFileProcessed = function(config, thisBatchId, s3Info) {
		var itemEntry = s3Info.bucket + '/' + s3Info.key;

		// perform the idempotency check for the file before we put it into
		// a manifest
		var fileEntry = {
			Item : {
				loadFile : {
					S : itemEntry
				}
			},
			Expected : {
				loadFile : {
					Exists : false
				}
			},
			TableName : filesTable
		};

		// add the file to the processed list
		dynamoDB.putItem(fileEntry, function(err, data) {
			if (err) {
				// the conditional check failed so the file has already been
				// processed
				console.log("File " + itemEntry + " Already Processed");
				context.done(null, null);
			} else {
				if (!data) {
					var msg = "Idempotency Check on " + fileEntry + " failed";
					console.log(msg);
					failBatch(msg, config, thisBatchId, s3Info, undefined);
				} else {
					// add was OK - proceed with adding the entry to the
					// pending batch
					addFileToPendingBatch(config, thisBatchId, s3Info, itemEntry);
				}
			}
		});
	};

	/**
	 * Function run to add a file to the existing open batch. This will repeatedly
	 * try to write and if unsuccessful it will requery the batch ID on the
	 * configuration
	 */
	var addFileToPendingBatch = function(config, thisBatchId, s3Info, itemEntry) {
		console.log("Adding Manifest Entry for " + itemEntry);

		var proceed = false;
		var asyncError = undefined;
		var addFileRetryLimit = 100;
		var tryNumber = 0;

		async
				.whilst(
						function() {
							// return OK if the proceed flag has been set, or if
							// we've hit the
							// retry count
							return !proceed && tryNumber < addFileRetryLimit;
						},
						function(callback) {
							tryNumber++;

							// build the reference to the pending batch, with an
							// atomic
							// add of the current file
							var item = {
								Key : {
									batchId : {
										S : thisBatchId
									},
									s3Prefix : {
										S : s3Info.prefix
									}
								},
								TableName : batchTable,
								UpdateExpression : "add entries :entry set #stat = :open, lastUpdate = :updateTime",
								ExpressionAttributeNames : {
									"#stat" : 'status'
								},
								ExpressionAttributeValues : {
									":entry" : {
										SS : [ itemEntry ]
									},
									":updateTime" : {
										N : '' + common.now(),
									},
									":open" : {
										S : open
									}
								},
								/* current batch can't be locked */
								ConditionExpression : "#stat = :open or attribute_not_exists(#stat)"
							};

							// add the file to the pending batch
							dynamoDB.updateItem(item, function(err, data) {
								if (err) {
									if (err.code === conditionCheckFailed) {
										// the batch I have a
										// reference to was
										// locked so reload
										// the current batch ID
										// from the config
										var configReloadRequest = {
											Key : {
												s3Prefix : {
													S : s3Info.prefix
												}
											},
											TableName : configTable,
											ConsistentRead : true
										};
										dynamoDB.getItem(configReloadRequest, function(err, data) {
											if (err) {
												console.log(err);
												callback(err);
											} else {
												/*
												 * reset the batch ID to the current marked batch
												 */
												thisBatchId = data.Item.currentBatch.S;

												/*
												 * we've not set proceed to true, so async will retry
												 */
												console.log("Reload of Configuration Complete after attempting Locked Batch Write");

												/*
												 * we can call into the callback immediately, as we
												 * probably just missed the pending batch processor's
												 * rotate of the configuration batch ID
												 */
												callback();
											}
										});
									} else {
										asyncError = err;
										proceed = true;
										callback();
									}
								} else {
									/*
									 * no error - the file was added to the batch, so mark the
									 * operation as OK so async will not retry
									 */
									proceed = true;
									callback();
								}
							});
						},
						function(err) {
							if (err) {
								// throw presented errors
								console.log(err);
								context.done(error, err);
							} else {
								if (asyncError) {
									// throw errors which were encountered
									// during the async
									// calls
									console.log(asyncError);
									context.done(error, asyncError);
								} else {
									if (!proceed) {
										// process what happened if the
										// iterative request to
										// write to the open pending batch timed
										// out
										//
										// TODO Can we force a rotation of the
										// current batch at
										// this point?
										var e = "Unable to write "
												+ itemEntry
												+ " in "
												+ addFileRetryLimit
												+ " attempts. Failing further processing to Batch "
												+ thisBatchId
												+ " which may be stuck in '"
												+ locked
												+ "' state. If so, unlock the back using `node unlockBatch.js <batch ID>`, delete the processed file marker with `node processedFiles.js -d <filename>`, and then re-store the file in S3";
										console.log(e);
										sendSNS(config.failureTopicARN.S, "Lambda Redshift Loader unable to write to Open Pending Batch",
												e, function() {
													context.done(error, e);
												}, function(err) {
													console.log(err);
													context.done(error, "Unable to Send SNS Notification");
												});
									} else {
										// the add of the file was successful,
										// so we
										linkProcessedFileToBatch(itemEntry, thisBatchId);
										// which is async, so may fail but we'll
										// still sweep the pending batch
										processPendingBatch(config, thisBatchId, s3Info);
									}
								}
							}
						});
	};

	/**
	 * Function which will link the deduplication table entry for the file to the
	 * batch into which the file was finally added
	 */
	var linkProcessedFileToBatch = function(itemEntry, batchId) {
		var updateProcessedFile = {
			Key : {
				loadFile : {
					S : itemEntry
				}
			},
			TableName : filesTable,
			AttributeUpdates : {
				batchId : {
					Action : 'PUT',
					Value : {
						S : batchId
					}
				}
			}
		};
		dynamoDB.updateItem(updateProcessedFile, function(err, data) {
			// because this is an async call which doesn't affect process flow,
			// we'll just log the error and do nothing with the OK response
			if (err) {
				console.log(err);
			}
		});
	};

	/**
	 * Function which links the manifest name used to load redshift onto the batch
	 * table entry
	 */
	var addManifestToBatch = function(config, thisBatchId, s3Info, manifestInfo) {
		// build the reference to the pending batch, with an atomic
		// add of the current file
		var item = {
			Key : {
				batchId : {
					S : thisBatchId
				},
				s3Prefix : {
					S : s3Info.prefix
				}
			},
			TableName : batchTable,
			AttributeUpdates : {
				manifestFile : {
					Action : 'PUT',
					Value : {
						S : manifestInfo.manifestPath
					}
				},
				lastUpdate : {
					Action : 'PUT',
					Value : {
						N : '' + common.now()
					}
				}
			}
		};

		dynamoDB.updateItem(item, function(err, data) {
			if (err) {
				console.log(err);
			} else {
				console.log("Linked Manifest " + manifestInfo.manifestName + " to Batch " + thisBatchId);
			}
		});
	};

	/**
	 * Function to process the current pending batch, and create a batch load
	 * process if required on the basis of size or timeout
	 */
	var processPendingBatch = function(config, thisBatchId, s3Info) {
		// make the request for the current batch
		var currentBatchRequest = {
			Key : {
				batchId : {
					S : thisBatchId
				},
				s3Prefix : {
					S : s3Info.prefix
				}
			},
			TableName : batchTable,
			ConsistentRead : true
		};

		dynamoDB.getItem(currentBatchRequest, function(err, data) {
			if (err) {
				console.log(err);
				context.done(error, err);
			} else if (!data || !data.Item) {
				var msg = "No open pending Batch " + thisBatchId;
				console.log(msg);
				context.done(null, msg);
			} else {
				// check whether the current batch is bigger than
				// the configured max size, or older than configured max age
				var lastUpdateTime = data.Item.lastUpdate.N;
				var pendingEntries = data.Item.entries.SS;
				var doProcessBatch = false;
				if (pendingEntries.length >= parseInt(config.batchSize.N)) {
					console.log("Batch Size " + config.batchSize.N + " reached");
					doProcessBatch = true;
				}

				if (config.batchTimeoutSecs && config.batchTimeoutSecs.N) {
					if (common.now() - lastUpdateTime > parseInt(config.batchTimeoutSecs.N) && pendingEntries.length > 0) {
						console.log("Batch Size " + config.batchSize.N + " not reached but reached Age "
								+ config.batchTimeoutSecs.N + " seconds");
						doProcessBatch = true;
					}
				}

				if (doProcessBatch) {
					// set the current batch to locked status
					var updateCurrentBatchStatus = {
						Key : {
							batchId : {
								S : thisBatchId,
							},
							s3Prefix : {
								S : s3Info.prefix
							}
						},
						TableName : batchTable,
						AttributeUpdates : {
							status : {
								Action : 'PUT',
								Value : {
									S : locked
								}
							},
							lastUpdate : {
								Action : 'PUT',
								Value : {
									N : '' + common.now()
								}
							}
						},
						// the batch to be processed has to be 'open',
						// otherwise we'll have multiple processes all
						// handling a single batch
						Expected : {
							status : {
								AttributeValueList : [ {
									S : open
								} ],
								ComparisonOperator : 'EQ'
							}
						},
						// add the ALL_NEW return values so we have the
						// most
						// up to date version of the entries string set
						ReturnValues : "ALL_NEW"
					};
					dynamoDB.updateItem(updateCurrentBatchStatus, function(err, data) {
						if (err) {
							if (err.code === conditionCheckFailed) {
								// some other Lambda function has locked
								// the batch - this is OK and we'll just
								// exit quietly
								context.done(null, null);
							} else {
								console.log("Unable to lock Batch " + thisBatchId);
								context.done(error, err);
							}
						} else {
							if (!data.Attributes) {
								var e = "Unable to extract latest pending entries set from Locked batch";
								console.log(e);
								context.done(error, e);
							} else {
								// grab the pending entries from the
								// locked batch
								pendingEntries = data.Attributes.entries.SS;

								// assign the loaded configuration a new batch
								// ID
								var allocateNewBatchRequest = {
									Key : {
										s3Prefix : {
											S : s3Info.prefix
										}
									},
									TableName : configTable,
									AttributeUpdates : {
										currentBatch : {
											Action : 'PUT',
											Value : {
												S : uuid.v4()
											}
										},
										lastBatchRotation : {
											Action : 'PUT',
											Value : {
												N : '' + common.now()
											}
										}
									}
								};

								dynamoDB.updateItem(allocateNewBatchRequest, function(err, data) {
									if (err) {
										console.log("Error while allocating new Pending Batch ID");
										console.log(err);
										context.done(error, err);
									} else {
										// OK - let's create the manifest file
										createManifest(config, thisBatchId, s3Info, pendingEntries);
									}
								});
							}
						}
					});
				} else {
					console.log("No pending batch flush required");
					context.done(null, null);
				}
			}
		});
	};

	/** Function which will create the manifest for a given batch and entries */
	var createManifest = function(config, thisBatchId, s3Info, batchEntries) {
		console.log("Creating Manifest for Batch " + thisBatchId);

		var manifestInfo = common.createManifestInfo(config);

		// create the manifest file for the file to be loaded
		var manifestContents = {
			entries : []
		};

		for (var i = 0; i < batchEntries.length; i++) {
			manifestContents.entries.push({
				// fix url encoding for files with spaces. Space values come in from
				// Lambda with '+' and plus values come in as %2B. Redshift wants the
				// original S3 value
				url : 's3://' + batchEntries[i].replace('+', ' ').replace('%2B', '+'),
				mandatory : true
			});
		}

		var s3PutParams = {
			Bucket : manifestInfo.manifestBucket,
			Key : manifestInfo.manifestPrefix,
			Body : JSON.stringify(manifestContents)
		};

		console.log("Writing manifest to " + manifestInfo.manifestBucket + "/" + manifestInfo.manifestPrefix);

		// save the manifest file to S3 and build the rest of the copy
		// command in the callback letting us know that the manifest was created
		// correctly
		s3.putObject(s3PutParams, loadRedshiftWithManifest.bind(undefined, config, thisBatchId, s3Info, manifestInfo));
	};

	/** Function run when the Redshift manifest write completes succesfully */
	var loadRedshiftWithManifest = function(config, thisBatchId, s3Info, manifestInfo, err, data) {
		if (err) {
			console.log("Error on Manifest Creation");
			console.log(err);
			context.done(error, err);
		} else {
			console.log("Created Manifest " + manifestInfo.manifestPath + " Successfully");

			// add the manifest file to the batch - this will NOT stop
			// processing if it fails
			addManifestToBatch(config, thisBatchId, s3Info, manifestInfo);

			/* build the redshift copy command */
			var copyCommand = '';

			// add the truncate option if requested
			if (config.truncateTarget && config.truncateTarget.BOOL) {
				copyCommand = 'truncate table ' + config.targetTable.S + ';\n';
			}

			var encryptedItems = [ kmsCrypto.stringToBuffer(config.secretKeyForS3.S),
					kmsCrypto.stringToBuffer(config.connectPassword.S) ];

			// decrypt the encrypted items
			kmsCrypto
					.decryptAll(
							encryptedItems,
							function(err, decryptedConfigItems) {
								if (err) {
									console.log("Unable to decrypt configuration items due to");
									console.log(err);
									context.done(error, err);
								} else {
									copyCommand = copyCommand + 'begin;\nCOPY ' + config.targetTable.S + ' from \'s3://'
											+ manifestInfo.manifestPath + '\' with credentials as \'aws_access_key_id='
											+ config.accessKeyForS3.S + ';aws_secret_access_key=' + decryptedConfigItems[0].toString()
											+ '\' manifest ';

									// add data formatting directives
									if (config.dataFormat.S === 'CSV') {
										copyCommand = copyCommand + ' delimiter \'' + config.csvDelimiter.S + '\'\n';
									} else if (config.dataFormat.S === 'JSON') {
										if (config.jsonPath !== undefined) {
											copyCommand = copyCommand + 'json \'' + config.jsonPath.S + '\'\n';
										} else {
											copyCommand = copyCommand + 'json \'auto\' \n';
										}
									} else {
										context.done(error, 'Unsupported data format ' + config.dataFormat.S);
									}

									// add compression directives
									if (config.compression !== undefined) {
										copyCommand = copyCommand + ' ' + config.compression.S + '\n';
									}

									// add copy options
									if (config.copyOptions !== undefined) {
										copyCommand = copyCommand + config.copyOptions.S + '\n';
									}

									// commit
									copyCommand = copyCommand + ";\ncommit;";

									// build the connection string
									var dbString = '';
									if (config.clusterDB) {
										dbString = '/' + config.clusterDB.S;
									}
									var clusterString = 'jdbc:postgresql://' + config.clusterEndpoint.S + ':' + config.clusterPort.N
											+ dbString + '?tcpKeepAlive=true';
									var dbConfig = {
										libpath : __dirname
												+ '/lib/postgresql-9.3-1102.jdbc41.jar:/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.75.x86_64/jre/lib/amd64/server/libjvm.so',
										drivername : 'org.postgresql.Driver',
										url : clusterString,
										user : config.connectUser.S,
										password : decryptedConfigItems[1].toString()
									};

									console.log("Connecting to Database " + clusterString);

									/* connect to database and run the commands */
									jdbc.initialize(dbConfig, function(err, res) {
										if (err) {
											failBatch(err, config, thisBatchId, s3Info, manifestInfo);
										} else {
											jdbc.open(function(err, conn) {
												if (err) {
													failBatch(err, config, thisBatchId, s3Info, manifestInfo);
												} else {
													if (conn) {
														jdbc.executeUpdate(copyCommand, function(err, result) {
															if (err) {
																failBatch(err, config, thisBatchId, s3Info, manifestInfo);
															} else {
																console.log("Load Complete");

																// close connection
																jdbc.close(function() {});

																// mark the batch as closed OK
																closeBatch(config, thisBatchId, s3Info, null);
															}
														});
													} else {
														failBatch('Unable to Connect to Database', config, thisBatchId, s3Info, manifestInfo);
													}
												}
											});
										}
									});
								}
							});
		}
	};

	/**
	 * Function which marks a batch as failed and sends notifications accordingly
	 */
	var failBatch = function(error, config, thisBatchId, s3Info, manifestInfo) {
		if (config.failedManifestKey && manifestInfo) {
			// copy the manifest to the failed location
			manifestInfo.failedManifestPrefix = manifestInfo.manifestPrefix.replace(manifestInfo.manifestKey + '/',
					config.failedManifestKey.S + '/');
			manifestInfo.failedManifestPath = manifestInfo.manifestBucket + '/' + manifestInfo.failedManifestPrefix;

			var copySpec = {
				Bucket : manifestInfo.manifestBucket,
				Key : manifestInfo.failedManifestPrefix,
				CopySource : manifestInfo.manifestPath
			};
			s3.copyObject(copySpec, function(err, data) {
				if (err) {
					console.log(err);
					closeBatch(config, thisBatchId, s3Info, manifestInfo, err);
				} else {
					console.log('Created new Failed Manifest ' + manifestInfo.failedManifestPath);

					// update the batch entry showing the failed manifest
					// location
					var manifestModification = {
						Key : {
							batchId : {
								S : thisBatchId
							},
							s3Prefix : {
								S : s3Info.prefix
							}
						},
						TableName : batchTable,
						AttributeUpdates : {
							manifestFile : {
								Action : 'PUT',
								Value : {
									S : manifestInfo.failedManifestPath
								}
							},
							lastUpdate : {
								Action : 'PUT',
								Value : {
									N : '' + common.now()
								}
							}
						}
					};
					dynamoDB.updateItem(manifestModification, function(err, data) {
						if (err) {
							console.log(err);
							closeBatch(config, thisBatchId, s3Info, manifestInfo, err);
						} else {
							// close the batch with the original calling
							// error
							closeBatch(config, thisBatchId, s3Info, manifestInfo, error);
						}
					});
				}
			});
		} else {
			console.log('Not requesting copy of Manifest to Failed S3 Location');
			closeBatch(config, thisBatchId, s3Info, manifestInfo, error);
		}
	};

	/**
	 * Function which closes the batch to mark it as done, including notifications
	 */
	var closeBatch = function(config, thisBatchId, s3Info, manifestInfo, batchError) {
		var batchEndStatus;

		if (batchError && batchError !== null) {
			batchEndStatus = error;
		} else {
			batchEndStatus = complete;
		}

		var item = {
			Key : {
				batchId : {
					S : thisBatchId
				},
				s3Prefix : {
					S : s3Info.prefix
				}
			},
			TableName : batchTable,
			AttributeUpdates : {
				status : {
					Action : 'PUT',
					Value : {
						S : batchEndStatus
					}
				},
				lastUpdate : {
					Action : 'PUT',
					Value : {
						N : '' + common.now()
					}
				}
			}
		};

		// add the error message to the updates if we had one
		if (batchError && batchError !== null) {
			item.AttributeUpdates.errorMessage = {
				Action : 'PUT',
				Value : {
					S : batchError.toString()
				}
			};
		}

		// mark the batch as closed
		dynamoDB.updateItem(item, function(err, data) {
			// ugh, the batch closure didn't finish - this is not a good
			// place to be
			if (err) {
				console.log(err);
				context.done(error, err);
			} else {
				// send notifications
				notify(config, thisBatchId, s3Info, manifestInfo, batchError);
			}
		});
	};

	/** send an SNS message to a topic */
	var sendSNS = function(topic, subj, msg, successCallback, failureCallback) {
		var m = {
			Message : JSON.stringify(msg),
			Subject : subj,
			TopicArn : topic
		};

		sns.publish(m, function(err, data) {
			if (err) {
				if (failureCallback) {
					failureCallback(err);
				} else {
					console.log(err);
				}
			} else {
				if (successCallback) {
					successCallback();
				}
			}
		});
	};

	/** Send SNS notifications if configured for OK vs Failed status */
	var notify = function(config, thisBatchId, s3Info, manifestInfo, batchError) {
		var message;
		var statusMessage = batchError ? 'error' : 'ok';
		var errorMessage = batchError ? batchError.toString() : null;
		var messageBody = {
			error : errorMessage,
			status : statusMessage,
			batchId : thisBatchId,
			s3Prefix : s3Info.prefix
		};

		if (manifestInfo) {
			messageBody.originalManifest = manifestInfo.manifestPath;
			messageBody.failedManifest = manifestInfo.failedManifestPath;
		}

		if (batchError && batchError !== null) {
			console.log(batchError);

			if (config.failureTopicARN) {
				sendSNS(config.failureTopicARN.S, "Lambda Redshift Batch Load " + thisBatchId + " Failure", messageBody,
						function() {
							context.done(error, batchError);
						}, function(err) {
							console.log(err);
							context.done(error, err);
						});
			} else {
				context.done(error, batchError);
			}
		} else {
			if (config.successTopicARN) {
				sendSNS(config.successTopicARN.S, "Lambda Redshift Batch Load " + thisBatchId + " OK", messageBody, function() {
					context.done(null, null);
				}, function(err) {
					console.log(err);
					context.done(error, err);
				});
			} else {
				// finished OK - no SNS notifications for success
				console.log("Batch Load " + thisBatchId + " Complete");
				context.done(null, null);
			}
		}
	};
	/* end of runtime functions */

	// commented out event logger, for debugging if needed
	// console.log(JSON.stringify(event));
	if (event.Records.length > 1) {
		context.done(error, "Unable to process multi-record events");
	} else {
		for (var i = 0; i < event.Records.length; i++) {
			var r = event.Records[i];

			// ensure that we can process this event based on a variety of criteria
			var noProcessReason = undefined;
			if (r.eventSource !== "aws:s3") {
				noProcessReason = "Invalid Event Source " + r.eventSource;
			}
			if (!(r.eventName === "ObjectCreated:Copy" || r.eventName === "ObjectCreated:Put")) {
				noProcessReason = "Invalid Event Name " + r.eventName;
			}
			if (r.s3.s3SchemaVersion !== "1.0") {
				noProcessReason = "Unknown S3 Schema Version " + r.s3.s3SchemaVersion;
			}

			if (noProcessReason) {
				console.log(noProcessReason);
				context.done(error, noProcessReason);
			} else {
				// extract the s3 details from the event
				var inputInfo = {
					bucket : undefined,
					key : undefined,
					prefix : undefined,
					inputFilename : undefined
				};

				inputInfo.bucket = r.s3.bucket.name;
				inputInfo.key = r.s3.object.key;

				// remove the bucket name from the key, if we have received it
				// - happens on object copy
				inputInfo.key = inputInfo.key.replace(inputInfo.bucket + "/", "");

				var keyComponents = inputInfo.key.split('/');
				inputInfo.inputFilename = keyComponents[keyComponents.length - 1];

				// remove the filename from the prefix value
				var searchKey = inputInfo.key.replace(inputInfo.inputFilename, '').replace(/\/$/, '');;

				// if the event didn't have a prefix, and is just in the bucket, then
				// just use the bucket name, otherwise add the prefix
				if (searchKey !== "") {
					searchKey = "/" + searchKey;
				}
				inputInfo.prefix = inputInfo.bucket + searchKey;

				// load the configuration for this prefix, which will kick off
				// the callback chain
				var dynamoLookup = {
					Key : {
						s3Prefix : {
							S : inputInfo.prefix
						}
					},
					TableName : configTable,
					ConsistentRead : true
				};

				// lookup the configuration item, and run foundConfig on completion
				dynamoDB.getItem(dynamoLookup, foundConfig.bind(undefined, inputInfo));
			}
		}
	}
};