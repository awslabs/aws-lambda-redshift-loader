/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
var debug = (process.env['DEBUG'] === 'true');
var log_level = process.env['LOG_LEVEL'] || 'info';
var pjson = require('./package.json');
var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
    region = "us-east-1";
    console.log("AWS Lambda Redshift Database Loader using default region " + region);
}

var aws = require('aws-sdk');
aws.config.update({
    region: region
});
var https_proxy = process.env['https_proxy'];
if (https_proxy !== undefined && https_proxy !== "") {
    console.log("Using proxy server " + https_proxy);
    var proxy_agent = require('https-proxy-agent');
    aws.config.update({
        httpOptions: {agent: new proxy_agent(https_proxy)}
    });
}

var s3 = new aws.S3({
    apiVersion: '2006-03-01',
    region: region
});
var dynamoDB = new aws.DynamoDB({
    apiVersion: '2012-08-10',
    region: region
});
var sns = new aws.SNS({
    apiVersion: '2010-03-31',
    region: region
});
require('./constants');
var kmsCrypto = require('./kmsCrypto');
kmsCrypto.setRegion(region);
var common = require('./common');
var async = require('async');
var uuid = require('uuid');
const {Client} = require('pg');
const maxRetryMS = 200;
const winston = require('winston');

const logger = winston.createLogger({
    level: debug === true ? 'debug' : log_level,
    transports: [
        new winston.transports.Console({
            format: winston.format.simple()
        })
    ]
});

// empty import/invocation of the keepalive fix for node-postgres module
require('pg-ka-fix')();

var upgrade = require('./upgrades');

String.prototype.shortenPrefix = function () {
    var tokens = this.split("/");

    if (tokens && tokens.length > 0) {
        return tokens.slice(0, tokens.length - 1).join("/");
    }
};

String.prototype.transformHiveStylePrefix = function () {
    // transform hive style dynamic prefixes into static
    // match prefixes

    var tokeniseSearchKey = this.split('/');
    var regex = /\=(.*)/;

    var processedTokens = tokeniseSearchKey.map(function (item) {
        if (item) {
            return item.replace(regex, "=*");
        }
    });

    return processedTokens.join('/');
};

function getConfigWithRetry(prefix, callback) {
    var proceed = false;
    var lookupConfigTries = 10;
    var tryNumber = 0;
    var configData = null;

    var dynamoLookup = {
        Key: {
            s3Prefix: {
                S: prefix
            }
        },
        TableName: configTable,
        ConsistentRead: true
    };

    logger.debug(JSON.stringify(dynamoLookup));

    async.whilst(function (test_cb) {
        // return OK if the proceed flag has been set, or if
        // we've hit the retry count
        test_cb(null, !proceed && tryNumber < lookupConfigTries);
    }, function (callback) {
        tryNumber++;

        logger.debug("Fetching S3 Configuration: Try " + tryNumber);

        // lookup the configuration item, and run foundConfig on completion
        dynamoDB.getItem(dynamoLookup, function (err, data) {
            if (err) {
                if (err.code === provisionedThroughputExceeded) {
                    // sleep for bounded jitter time up to 1
                    // second and then retry
                    var timeout = common.randomInt(0, 1000);
                    logger.info(provisionedThroughputExceeded + " while accessing " + configTable + ". Retrying in " + timeout + " ms");
                    setTimeout(callback, timeout);
                } else {
                    // some other error - call the error callback
                    callback(err);
                }
            } else {
                configData = data;
                proceed = true;
                callback(null);
            }
        });
    }, function (err) {
        if (err) {
            logger.error(err);
            callback(err);
        } else {
            callback(null, configData);
        }
    });
};
exports.getConfigWithRetry = getConfigWithRetry;

function resolveConfig(prefix, successCallback, noConfigFoundCallback) {
    var searchPrefix = prefix;
    var config;

    async.until(function (test_cb) {
        // run until we have found a configuration item, or the search
        // prefix is undefined due to the shortening being completed
        test_cb(null, config || !searchPrefix);
    }, function (untilCallback) {
        // query for the prefix, implementing a reduce by '/' each time,
        // such that we load the most specific config first
        logger.debug("Extracting S3 Config for Path: " + searchPrefix);

        getConfigWithRetry(searchPrefix, function (err, data) {
            if (err) {
                untilCallback(err);
            } else {
                if (data.Item) {
                    // set the config = this will cause the 'until' to complete
                    config = data;
                } else {
                    // reduce the search prefix by one prefix item
                    searchPrefix = searchPrefix.shortenPrefix();
                }
                untilCallback();
            }
        });
    }, function (err) {
        if (err) {
            noConfigFoundCallback(err)
        } else {
            if (config) {
                successCallback(err, config);
            } else {
                noConfigFoundCallback(err);
            }
        }
    });
};
exports.resolveConfig = resolveConfig;


// main function for AWS Lambda
function handler(event, context) {
    /** runtime functions * */

    /*
	 * Function which performs all version upgrades over time - must be able to
	 * do a forward migration from any version to 'current' at all times!
	 */
    function upgradeConfig(s3Info, currentConfig, callback) {
        // v 1.x to 2.x upgrade for multi-cluster loaders
        if (currentConfig.version !== pjson.version) {
            logger.debug(`Performing version upgrade from ${currentConfig.version} to ${pjson.version}`);
            upgrade.upgradeAll(dynamoDB, s3Info, currentConfig, callback);
        } else {
            // no upgrade needed
            callback(null, s3Info, currentConfig);
        }
    }

    /* callback run when we find a configuration for load in Dynamo DB */
    function foundConfig(s3Info, err, data) {
        if (err) {
            logger.error(err);
            var msg = `Error getting Redshift Configuration for ${s3Info.prefix} from DynamoDB`;
            logger.error(msg);
            context.done(error, msg);
        }

        logger.info(`Found Redshift Load Configuration for ${s3Info.prefix}`);

        var config = data.Item;
        var thisBatchId = config.currentBatch.S;

        // run all configuration upgrades required
        upgradeConfig(s3Info, config, function (err, s3Info, useConfig) {
            if (err) {
                console.error(JSON.stringify(err));
                context.done(error, JSON.stringify(err));
            } else {
                if (useConfig.filenameFilterRegex) {
                    var isFilterRegexMatch = true;

                    try {
                        isFilterRegexMatch = s3Info.key.match(useConfig.filenameFilterRegex.S);
                    } catch (e) {
                        // suppress this error - it may have been a malformed
                        // regex as well as just a non-match
                        // exceptions are treated as a file match here - we'd
                        // rather process a file and have the batch
                        // fail than erroneously ignore it
                        logger.error("Error on filename filter evaluation. File will be included for processing");
                        logger.error(e);
                    }
                    if (isFilterRegexMatch) {
                        checkFileProcessed(useConfig, thisBatchId, s3Info);
                    } else {
                        logger.info('Object ' + s3Info.key + ' excluded by filename filter \'' + useConfig.filenameFilterRegex.S + '\'');

                        // scan the current batch to decide if it needs to be
                        // flushed due to batch timeout
                        processPendingBatch(useConfig, thisBatchId, s3Info);
                    }
                } else {
                    // no filter, so we'll load the data
                    checkFileProcessed(useConfig, thisBatchId, s3Info);
                }
            }
        });
    };

    /*
	 * function to add a file to the pending batch set and then call the success
	 * callback
	 */
    function checkFileProcessed(config, thisBatchId, s3Info) {
        var itemEntry = s3Info.bucket + '/' + s3Info.key;

        // perform the idempotency check for the file before we put it
        // into a manifest
        var fileEntry = {
            Key: {
                loadFile: {
                    S: itemEntry
                }
            },
            TableName: filesTable,
            ExpressionAttributeNames: {
                "#rcvDate": "receiveDateTime",
                "#ttl": "timeToLive"
            },
            ExpressionAttributeValues: {
                ":rcvDate": {
                    S: common.readableTime(common.now())
                },
                ":ttl":  {
                    N: '' + Math.floor(Date.now() / 1000) + 60 * 60
                },
                ":incr": {
                    N: "1"
                }
            },
            UpdateExpression: "set #rcvDate = :rcvDate,  #ttl = :ttl add timesReceived :incr",
            ReturnValues: "ALL_NEW"
        };

        logger.debug("Checking whether File is already processed");
        logger.debug(JSON.stringify(fileEntry));

        // add the file to the processed list
        dynamoDB.updateItem(fileEntry, function (err, data) {
            var msg;
            if (err) {
                msg = "Error " + err.code + " for " + fileEntry;
                logger.error(msg);
                context.done(error, msg);
            } else {
                if (!data) {
                    msg = "Update failed to return data from Processed File Check";
                    logger.error(msg);
                    context.done(error, msg);
                } else {
                    if (data.Attributes.batchId && data.Attributes.batchId.S) {
                        // there's already a pending batch link, so this is a
                        // full duplicate and we'll discard
                        logger.info("File " + itemEntry + " Already Processed");
                        context.done(null, null);
                    } else {
                        // update was successful, and either this is the first
                        // event and there was no batch ID
                        // specified, or the file is a reprocess but the batch
                        // ID attachment didn't work - proceed
                        // with adding the entry to the pending batch
                        addFileToPendingBatch(config, thisBatchId, s3Info, itemEntry);
                    }
                }
            }
        });
    };

    /**
     * Function run to add a file to the existing open batch. This will
     * repeatedly try to write and if unsuccessful it will requery the batch ID
     * on the configuration
     */
    function addFileToPendingBatch(config, thisBatchId, s3Info, itemEntry) {
        console.log("Adding Pending Batch Entry for " + itemEntry);

        var proceed = false;
        var asyncError;
        var addFileRetryLimit = 100;
        var tryNumber = 0;
        var configReloads = 0;

        async.whilst(
            function (test_cb) {
                // return OK if the proceed flag has been set, or if we've hit
                // the retry count
                test_cb(null, !proceed && tryNumber < addFileRetryLimit);
            },
            function (callback) {
                tryNumber++;

                // build the reference to the pending batch, with an atomic add
                // of the current file
                var now = common.now();
                var item = {
                    Key: {
                        batchId: {
                            S: thisBatchId
                        },
                        s3Prefix: {
                            S: s3Info.prefix
                        }
                    },
                    TableName: batchTable,
                    UpdateExpression: "add entries :entry, writeDates :appendFileDate, size :size set #stat = :open, lastUpdate = :updateTime",
                    ExpressionAttributeNames: {
                        "#stat": 'status'
                    },
                    ExpressionAttributeValues: {
                        ":entry": {
                            SS: [itemEntry]
                        },
                        ":appendFileDate": {
                            NS: ['' + now]
                        },
                        ":updateTime": {
                            N: '' + now
                        },
                        ":open": {
                            S: open
                        },
                        ":size": {
                            N: '' + s3Info.size
                        },
                        ":ttl":  {
                            N: '' + Math.floor(Date.now() / 1000) + 60 * 60
                        }
                    },
                    /*
					 * current batch can't be locked
					 */
                    ConditionExpression: "#stat = :open or attribute_not_exists(#stat)"
                };

                // add the file to the pending batch
                dynamoDB.updateItem(item, function (err, data) {
                    if (err) {
                        let waitFor = Math.min(Math.pow(tryNumber, 2) * 10, maxRetryMS);

                        if (err.code === provisionedThroughputExceeded) {
                            logger.warn("Provisioned Throughput Exceeded on addition of " + s3Info.prefix + " to pending batch " + thisBatchId + ". Trying again in " + waitFor + " ms");
                            setTimeout(callback, waitFor);
                        } else if (err.code === conditionCheckFailed) {
                            // the batch I have a reference to was locked so
                            // reload the current batch ID from the config
                            var configReloadRequest = {
                                Key: {
                                    s3Prefix: {
                                        S: s3Info.prefix
                                    }
                                },
                                TableName: configTable,
                                /*
								 * we need a consistent read here to ensure we
								 * get the latest batch ID
								 */
                                ConsistentRead: true
                            };
                            dynamoDB.getItem(configReloadRequest, function (err, data) {
                                configReloads++;
                                if (err) {
                                    if (err === provisionedThroughputExceeded) {
                                        logger.warn("Provisioned Throughput Exceeded on reload of " + configTable + " due to locked batch write");
                                        callback();
                                    } else {
                                        console.log(err);
                                        callback(err);
                                    }
                                } else {
                                    if (data.Item.currentBatch.S === thisBatchId) {
                                        // we've obtained the same batch ID back
                                        // from the configuration as we have
                                        // now, meaning it hasn't yet rotated
                                        logger.warn("Batch " + thisBatchId + " still current after configuration reload attempt " + configReloads + ". Recycling in " + waitFor + " ms.");

                                        // because the batch hasn't been
                                        // reloaded on the configuration, we'll
                                        // backoff here for a moment to let that
                                        // happen
                                        setTimeout(callback, waitFor);
                                    } else {
                                        // we've got an updated batch id, so use
                                        // this in the next cycle of file add
                                        thisBatchId = data.Item.currentBatch.S;

                                        logger.warn("Obtained new Batch ID " + thisBatchId + " after configuration reload. Attempt " + configReloads);

                                        /*
										 * callback immediately, as we should
										 * now have a valid and open batch to
										 * use
										 */
                                        callback();
                                    }
                                }
                            });
                        } else {
                            asyncError = err;
                            proceed = true;
                            callback();
                        }
                    } else {
                        // no error - the file was added to the batch, so mark
                        // the operation as OK so async will not retry
                        proceed = true;
                        callback();
                    }
                });
            },
            function (err) {
                if (err) {
                    // throw presented errors
                    logger.error(JSON.stringify(err));
                    context.done(error, JSON.stringify(err));
                } else {
                    if (asyncError) {
                        /*
						 * throw errors which were encountered during the async
						 * calls
						 */
                        logger.error(JSON.stringify(asyncError));
                        context.done(error, JSON.stringify(asyncError));
                    } else {
                        if (!proceed) {
                            /*
							 * process what happened if the iterative request to
							 * write to the open pending batch timed out
							 * 
							 * TODO Can we force a rotation of the current batch
							 * at this point?
							 */
                            var e = "Unable to write "
                                + itemEntry
                                + " in "
                                + addFileRetryLimit
                                + " attempts. Failing further processing to Batch "
                                + thisBatchId
                                + " which may be stuck in '"
                                + locked
                                + "' state. If so, unlock the back using `node unlockBatch.js <batch ID>`, delete the processed file marker with `node processedFiles.js -d <filename>`, and then re-store the file in S3";
                            logger.error(e);

                            var msg = "Lambda Redshift Loader unable to write to Open Pending Batch";

                            if (config.failureTopicARN) {
                                sendSNS(config.failureTopicARN.S, msg, e, function () {
                                    context.done(error, e);
                                }, function (err) {
                                    logger.error(err);
                                    context.done(error, "Unable to Send SNS Notification");
                                });
                            } else {
                                logger.error("Unable to send failure notifications");
                                logger.error(msg);
                                context.done(error, msg);
                            }
                        } else {
                            // the add of the file was successful,
                            // so we
                            linkProcessedFileToBatch(itemEntry, thisBatchId);
                            // which is async, so may fail but we'll
                            // still sweep
                            // the pending batch
                            processPendingBatch(config, thisBatchId, s3Info);
                        }
                    }
                }
            });
    };

    /**
     * Function which will link the deduplication table entry for the file to
     * the batch into which the file was finally added
     */
    function linkProcessedFileToBatch(itemEntry, batchId) {
        var updateProcessedFile = {
            Key: {
                loadFile: {
                    S: itemEntry
                }
            },
            TableName: filesTable,
            AttributeUpdates: {
                batchId: {
                    Action: 'PUT',
                    Value: {
                        S: batchId
                    }
                },
                ttl:  {
                    Action: 'PUT',
                    Value: {
                        N: '' + Math.floor(Date.now() / 1000) + 60 * 60
                    }
                }
            }
        };

        logger.debug("Linking file to current batch");
        logger.debug(JSON.stringify(updateProcessedFile));

        common.retryableUpdate(dynamoDB, updateProcessedFile, function (err, data) {
            // because this is an async call which doesn't affect
            // process flow, we'll just log the error and do nothing with the OK
            // response
            if (err) {
                logger.error(err);
            }
        });
    };

    /**
     * Function which links the manifest name used to load redshift onto the
     * batch table entry
     */
    function addManifestToBatch(config, thisBatchId, s3Info, manifestInfo) {
        // build the reference to the pending batch, with an atomic
        // add of the current file
        var item = {
            Key: {
                batchId: {
                    S: thisBatchId
                },
                s3Prefix: {
                    S: s3Info.prefix
                }
            },
            TableName: batchTable,
            AttributeUpdates: {
                manifestFile: {
                    Action: 'PUT',
                    Value: {
                        S: manifestInfo.manifestPath
                    }
                },
                lastUpdate: {
                    Action: 'PUT',
                    Value: {
                        N: '' + common.now()
                    }
                },
                ttl:  {
                    Action: 'PUT',
                    Value: {
                        N: '' + Math.floor(Date.now() / 1000) + 60 * 60
                    }
                }
            }
        };

        logger.debug("Linking manifest file pointer to Batch");
        logger.debug(JSON.stringify(item));

        common.retryableUpdate(dynamoDB, item, function (err, data) {
            if (err) {
                logger.error(err);
            } else {
                logger.info("Linked Manifest " + manifestInfo.manifestName + " to Batch " + thisBatchId);
            }
        });
    };

    /**
     * Function to process the current pending batch, and create a batch load
     * process if required on the basis of size or timeout
     */
    function processPendingBatch(config, thisBatchId, s3Info) {
        // make the request for the current batch
        var currentBatchRequest = {
            Key: {
                batchId: {
                    S: thisBatchId
                },
                s3Prefix: {
                    S: s3Info.prefix
                }
            },
            TableName: batchTable,
            ConsistentRead: true
        };

        logger.debug("Loading current Batch record from prefix config");
        logger.debug(JSON.stringify(currentBatchRequest));

        dynamoDB.getItem(currentBatchRequest, function (err, data) {
            if (err) {
                if (err === provisionedThroughputExceeded) {
                    logger.warn("Provisioned Throughput Exceeded on read of " + batchTable);
                    callback();
                } else {
                    logger.error(JSON.stringify(err));
                    context.done(error, JSON.stringify(err));
                }
            } else if (!data || !data.Item) {
                var msg = "No open pending Batch " + thisBatchId;
                logger.error(msg);
                context.done(null, msg);
            } else {
                // first step is to resolve the earliest writeDate as the batch
                // creation date
                var batchCreateDate;
                data.Item.writeDates.NS.map(function (item) {
                    var t = parseInt(item);

                    logger.debug(`Batch entry epoch timestamp: ${item}`);

                    if (!batchCreateDate || t < batchCreateDate) {
                        batchCreateDate = t;
                    }
                });
                var lastUpdateTime = data.Item.lastUpdate.N;
                var pendingEntries = data.Item.entries.SS;
                var doProcessBatch = false;

                if (!pendingEntries || pendingEntries.length >= parseInt(config.batchSize.N)) {
                    logger.info("Batch count " + config.batchSize.N + " reached");
                    doProcessBatch = true;
                } else {
                    if (config.batchSize && config.batchSize.N) {
                        logger.debug("Current batch count of " + pendingEntries.length + " below batch limit of " + config.batchSize.N);
                    }
                }

                // check whether the current batch is bigger than the configured
                // max count, size, or older than configured max age
                if (config.batchTimeoutSecs && config.batchTimeoutSecs.N && pendingEntries.length > 0 && common.now() - batchCreateDate > parseInt(config.batchTimeoutSecs.N)) {
                    logger.info("Batch age " + config.batchTimeoutSecs.N + " seconds reached");
                    doProcessBatch = true;
                } else {
                    if (config.batchTimeoutSecs && config.batchTimeoutSecs.N) {
                        logger.debug("Current batch age of " + (common.now() - batchCreateDate) + " seconds below batch timeout: "
                            + (config.batchTimeoutSecs.N ? config.batchTimeoutSecs.N : "None Defined"));
                    }
                }

                if (config.batchSizeBytes && config.batchSizeBytes.N && pendingEntries.length > 0 && parseInt(config.batchSizeBytes.N) <= parseInt(data.Item.size.N)) {
                    logger.info("Batch size " + config.batchSizeBytes.N + " bytes reached");
                    doProcessBatch = true;
                } else {
                    if (data.Item.size.N) {
                        logger.debug("Current batch size of " + data.Item.size.N + " below batch threshold or not configured");
                    }
                }

                if (doProcessBatch) {
                    // set the current batch to locked status
                    var updateCurrentBatchStatus = {
                        Key: {
                            batchId: {
                                S: thisBatchId
                            },
                            s3Prefix: {
                                S: s3Info.prefix
                            }
                        },
                        TableName: batchTable,
                        AttributeUpdates: {
                            status: {
                                Action: 'PUT',
                                Value: {
                                    S: locked
                                }
                            },
                            lastUpdate: {
                                Action: 'PUT',
                                Value: {
                                    N: '' + common.now()
                                }
                            },
                            ttl:  {
                                Action: 'PUT',
                                Value: {
                                    N: '' + Math.floor(Date.now() / 1000) + 60 * 60
                                }
                            }
                        },
                        /*
						 * the batch to be processed has to be 'open', otherwise
						 * we'll have multiple processes all handling a single
						 * batch
						 */
                        Expected: {
                            status: {
                                AttributeValueList: [{
                                    S: open
                                }],
                                ComparisonOperator: 'EQ'
                            }
                        },
                        /*
						 * add the ALL_NEW return values so we have the most up
						 * to date version of the entries string set
						 */
                        ReturnValues: "ALL_NEW",
                    };

                    logger.debug("Attempting to lock Batch for processing");
                    logger.debug(JSON.stringify(updateCurrentBatchStatus));

                    common.retryableUpdate(dynamoDB, updateCurrentBatchStatus, function (err, data) {
                        if (err) {
                            if (err.code === conditionCheckFailed) {
                                // some other Lambda function has locked the
                                // batch - this is OK and we'll just exit
                                // quietly
                                logger.debug("Batch is ready to be processed, but another thread has locked it for loading");
                                context.done(null, null);
                            } else if (err.code === provisionedThroughputExceeded) {
                                logger.error("Provisioned Throughput Exceeded on " + batchTable + " while trying to lock Batch");
                                context.done(error, JSON.stringify(err));
                            } else {
                                logger.error("Unhandled exception while trying to lock Batch " + thisBatchId);
                                logger.error(JSON.stringify(err));
                                context.done(error, JSON.stringify(err));
                            }
                        } else {
                            if (!data || !data.Attributes) {
                                var e = "Unable to extract latest pending entries set from Locked batch";
                                logger.error(e);
                                context.done(error, e);
                            } else {
                                /*
								 * grab the pending entries from the locked
								 * batch
								 */
                                pendingEntries = data.Attributes.entries.SS;

                                /*
								 * assign the loaded configuration a new batch
								 * ID
								 */
                                var allocateNewBatchRequest = {
                                    Key: {
                                        s3Prefix: {
                                            S: s3Info.prefix
                                        }
                                    },
                                    TableName: configTable,
                                    AttributeUpdates: {
                                        currentBatch: {
                                            Action: 'PUT',
                                            Value: {
                                                S: uuid.v4()
                                            }
                                        },
                                        lastBatchRotation: {
                                            Action: 'PUT',
                                            Value: {
                                                S: common.getFormattedDate()
                                            }
                                        }
                                    }
                                };

                                logger.debug("Allocating new Batch ID for future processing");
                                logger.debug(JSON.stringify(allocateNewBatchRequest));

                                common.retryableUpdate(dynamoDB, allocateNewBatchRequest, function (err, data) {
                                    if (err) {
                                        logger.error("Error while allocating new Pending Batch ID");
                                        logger.error(JSON.stringify(err));
                                        context.done(error, JSON.stringify(err));
                                    } else {
                                        // OK - let's create the manifest file
                                        createManifest(config, thisBatchId, s3Info, pendingEntries);
                                    }
                                });
                            }
                        }
                    });
                } else {
                    logger.info("No pending batch flush required");
                    context.done(null, null);
                }
            }
        });
    };

    /**
     * Function which will create the manifest for a given batch and entries
     */
    function createManifest(config, thisBatchId, s3Info, batchEntries) {
        logger.info("Creating Manifest for Batch " + thisBatchId);

        var manifestInfo = common.createManifestInfo(config);

        // create the manifest file for the file to be loaded
        var manifestContents = {
            entries: []
        };

        logger.debug("Building new COPY Manifest");

        for (var i = 0; i < batchEntries.length; i++) {
            /*
			 * fix url encoding for files with spaces. Space values come in from
			 * Lambda with '+' and plus values come in as %2B. Redshift wants
			 * the original S3 value
			 */
            u = 's3://' + batchEntries[i].replace(/\+/g, ' ').replace(/%2B/g, '+')

            logger.debug(u);

            manifestContents.entries.push({
                url: u,
                mandatory: true,
                meta: {
                    content_length: s3Info.size
                }
            });
        }

        var s3PutParams = {
            Bucket: manifestInfo.manifestBucket,
            Key: manifestInfo.manifestPrefix,
            Body: JSON.stringify(manifestContents)
        };

        logger.info("Writing manifest to " + manifestInfo.manifestBucket + "/" + manifestInfo.manifestPrefix);

        /*
		 * save the manifest file to S3 and build the rest of the copy command
		 * in the callback letting us know that the manifest was created
		 * correctly
		 */
        s3.putObject(s3PutParams, loadRedshiftWithManifest.bind(undefined, config, thisBatchId, s3Info, manifestInfo));
    };

    /**
     * Function run when the Redshift manifest write completes succesfully
     */
    function loadRedshiftWithManifest(config, thisBatchId, s3Info, manifestInfo, err, data) {
        if (err) {
            logger.error("Error on Manifest Creation");
            logger.error(err);
            failBatch(err, config, thisBatchId, s3Info, manifestInfo);
        } else {
            logger.info("Created Manifest " + manifestInfo.manifestPath + " Successfully");

            // add the manifest file to the batch - this will NOT stop
            // processing if it fails
            addManifestToBatch(config, thisBatchId, s3Info, manifestInfo);

            // convert the config.loadClusters list into a format that
            // looks like a native dynamo entry
            var clustersToLoad = [];
            for (var i = 0; i < config.loadClusters.L.length; i++) {
                clustersToLoad[clustersToLoad.length] = config.loadClusters.L[i].M;
            }

            logger.info("Loading " + clustersToLoad.length + " Clusters");

            // run all the cluster loaders in parallel
            async.map(clustersToLoad, function (item, callback) {
                // call the load cluster function, passing it the continuation
                // callback
                loadCluster(config, thisBatchId, s3Info, manifestInfo, item, callback);
            }, function (err, results) {
                if (err) {
                    logger.error(err);
                }

                // go through all the results - if they were all
                // OK, then close the batch OK - otherwise fail
                var allOK = true;
                var loadState = {};

                for (var i = 0; i < results.length; i++) {
                    if (!results[i] || results[i].status === ERROR) {
                        allOK = false;

                        logger.error("Cluster Load Failure " + results[i].error + " on Cluster " + results[i].cluster);
                    }
                    // log the response state for each cluster
                    loadState[results[i].cluster] = {
                        status: results[i].status,
                        error: results[i].error
                    };
                }

                var loadStateRequest = {
                    Key: {
                        batchId: {
                            S: thisBatchId
                        },
                        s3Prefix: {
                            S: s3Info.prefix
                        }
                    },
                    TableName: batchTable,
                    AttributeUpdates: {
                        clusterLoadStatus: {
                            Action: 'PUT',
                            Value: {
                                S: JSON.stringify(loadState)
                            }
                        },
                        lastUpdate: {
                            Action: 'PUT',
                            Value: {
                                N: '' + common.now()
                            }
                        },
                        ttl:  {
                            Action: 'PUT',
                            Value: {
                                N: '' + Math.floor(Date.now() / 1000) + 60 * 60
                            }
                        }
                    }
                };

                logger.debug("Linking Batch load state for cluster");
                logger.debug(JSON.stringify(loadStateRequest));

                common.retryableUpdate(dynamoDB, loadStateRequest, function (err, data) {
                    if (err) {
                        logger.error("Error while attaching per-Cluster Load State");
                        failBatch(err, config, thisBatchId, s3Info, manifestInfo);
                    } else {
                        if (allOK === true) {
                            // close the batch as OK
                            closeBatch(null, config, thisBatchId, s3Info, manifestInfo, loadState);
                        } else {
                            // close the batch as failure
                            failBatch(loadState, config, thisBatchId, s3Info, manifestInfo);
                        }
                    }
                });
            });
        }
    };

    /**
     * Function which will run a postgres command with retries
     */
    function runPgCommand(clusterInfo, client, command, retries, retryableErrorTraps, retryBackoffBaseMs, callback) {
        var completed = false;
        var retryCount = 0;
        var lastError;

        async.until(function (test_cb) {
            test_cb(null, completed || !retries || retryCount >= retries);
        }, function (asyncCallback) {
            logger.debug("Performing Database Command:");
            logger.debug(command);

            client.query(command, function (queryCommandErr, result) {
                if (queryCommandErr) {
                    lastError = queryCommandErr;
                    // check all the included retryable error traps to see if
                    // this is a retryable error
                    var retryable = false;
                    if (retryableErrorTraps) {
                        retryableErrorTraps.map(function (retryableError) {
                            if (queryCommandErr.detail && queryCommandErr.detail.indexOf(retryableError) > -1) {
                                retryable = true;
                            }
                        });
                    }

                    // if the error is not retryable, then fail by calling the
                    // async callback with the specified error
                    if (!retryable) {
                        completed = true;
                        if (queryCommandErr && queryCommandErr.detail) {
                            logger.error(queryCommandErr.detail);
                        }
                        asyncCallback(queryCommandErr);
                    } else {
                        // incre ment the retry count
                        retryCount += 1;

                        logger.warn("Retryable Error detected. Try Attempt " + retryCount);

                        // exponential backoff
                        // if a backoff time is
                        // provided
                        if (retryBackoffBaseMs) {
                            setTimeout(function () {
                                // call the async callback
                                asyncCallback(null);
                            }, Math.pow(2, retryCount) * retryBackoffBaseMs);
                        }
                    }
                } else {
                    completed = true;
                    asyncCallback(queryCommandErr);
                }
            });
        }, function (afterQueryCompletedErr) {
            // close the server connection
            client.end((disconnectErr) => {
                if (disconnectErr) {
                    logger.error("Error during server disconnect: " + disconnectErr.stack);
                    logger.error("Watch for database connection count increasing without limit!!!");
                }

                /*
				 * check the status of the query completion, but don't worry
				 * about disconnection errors here. we can't fix them, and
				 * hopefully the server will just close them effectively :/
				 */
                if (afterQueryCompletedErr) {
                    // callback as error
                    callback(null, {
                        status: ERROR,
                        error: afterQueryCompletedErr,
                        cluster: clusterInfo.clusterEndpoint.S
                    });
                } else {
                    if (!completed) {
                        // we were unable to complete the command
                        callback(null, {
                            status: ERROR,
                            error: lastError,
                            cluster: clusterInfo.clusterEndpoint.S
                        });
                    } else {
                        // command ok
                        callback(null, {
                            status: OK,
                            error: null,
                            cluster: clusterInfo.clusterEndpoint.S
                        });
                    }
                }
            });
        });
    };

    /**
     * Function which loads a redshift cluster
     *
     */
    function loadCluster(config, thisBatchId, s3Info, manifestInfo, clusterInfo, callback) {
        /* build the redshift copy command */
        var copyCommand = '';

        // set the statement timeout to 10 seconds less than the remaining
        // execution time on the lambda function, or 60 seconds if we can't
        // resolve the time remaining. fail the lambda function if we have less
        // than 5 seconds remaining
        var remainingMillis;
        if (context) {
            remainingMillis = context.getRemainingTimeInMillis();

            if (remainingMillis < 10000) {
                failBatch("Remaining duration of " + remainingMillis + ' insufficient to load cluster', config, thisBatchId, s3Info, manifestInfo);
            } else {
                copyCommand = 'set statement_timeout to ' + (remainingMillis - 10000) + ';\n';
            }
        } else {
            copyCommand = 'set statement_timeout to 60000;\n';
        }

        // open a transaction so that all pre-sql, load, and post-sql commit at
        // once
        copyCommand += 'begin;\n';

        // if the presql option is set, insert it into the copyCommand
        if (clusterInfo.presql && clusterInfo.presql.S) {
            copyCommand += clusterInfo.presql.S + (clusterInfo.presql.S.slice(-1) == ";" ? "" : ";") + '\n'
        }

        var copyOptions = "manifest ";

        // add the truncate option if requested
        if (clusterInfo.truncateTarget && clusterInfo.truncateTarget.BOOL) {
            copyCommand += 'truncate table ' + clusterInfo.targetTable.S + ';\n';
        }

        var encryptedItems = {};
        var useLambdaCredentialsToLoad = true;
        const s3secretKeyMapEntry = "s3secretKey";
        const passwordKeyMapEntry = "clusterPassword";
        const symmetricKeyMapEntry = "symmetricKey";

        if (config.secretKeyForS3) {
            encryptedItems[s3secretKeyMapEntry] = Buffer.from(config.secretKeyForS3.S, 'base64');
            useLambdaCredentialsToLoad = false;
        }

        logger.debug("Loading Cluster " + clusterInfo.clusterEndpoint.S + " with " + (useLambdaCredentialsToLoad === true ? "Lambda" : "configured") + " credentials");

        // add the cluster password
        encryptedItems[passwordKeyMapEntry] = Buffer.from(clusterInfo.connectPassword.S, 'base64');

        // add the master encryption key to the list of items to be decrypted,
        // if there is one
        if (config.masterSymmetricKey) {
            encryptedItems[symmetricKeyMapEntry] = Buffer.from(config.masterSymmetricKey.S, 'base64');
        }

        // decrypt the encrypted items
        kmsCrypto.decryptMap(encryptedItems, function (err, decryptedConfigItems) {
            if (err) {
                callback(err, {
                    status: ERROR,
                    cluster: clusterInfo.clusterEndpoint.S
                });
            } else {
                // create the credentials section
                var credentials;

                if (useLambdaCredentialsToLoad === true) {
                    credentials = 'aws_access_key_id=' + aws.config.credentials.accessKeyId + ';aws_secret_access_key=' + aws.config.credentials.secretAccessKey;

                    if (aws.config.credentials.sessionToken) {
                        credentials += ';token=' + aws.config.credentials.sessionToken;
                    }
                } else {
                    credentials = 'aws_access_key_id=' + config.accessKeyForS3.S + ';aws_secret_access_key=' + decryptedConfigItems[s3secretKeyMapEntry].toString();
                }

                if (typeof clusterInfo.columnList === 'undefined') {
                    copyCommand = copyCommand + 'COPY ' + clusterInfo.targetTable.S + ' from \'s3://' + manifestInfo.manifestPath + '\'';
                } else {
                    copyCommand = copyCommand + 'COPY ' + clusterInfo.targetTable.S + ' (' + clusterInfo.columnList.S + ') from \'s3://' + manifestInfo.manifestPath + '\'';
                }

                // add data formatting directives to copy
                // options
                if (config.dataFormat.S === 'CSV') {
                    // if removequotes or escape has been used in copy options, then we wont use the CSV formatter
                    if (!(config.copyOptions && (config.copyOptions.S.toUpperCase().indexOf('REMOVEQUOTES') > -1 || config.copyOptions.S.toUpperCase().indexOf('ESCAPE') > -1))) {
                        copyOptions = copyOptions + 'format csv ';
                    }

                    copyOptions = copyOptions + 'delimiter \'' + config.csvDelimiter.S + '\'\n';

                    // this will ignore the first line
                    if (config.ignoreCsvHeader && config.ignoreCsvHeader.BOOL) {
                        copyOptions = copyOptions + ' IGNOREHEADER 1 ' + '\n';
                    }

                } else if (config.dataFormat.S === 'JSON' || config.dataFormat.S === 'AVRO') {
                    copyOptions = copyOptions + ' format ' + config.dataFormat.S;

                    if (!(config.jsonPath === undefined || config.jsonPath === null)) {
                        copyOptions = copyOptions + ' \'' + config.jsonPath.S + '\' \n';
                    } else {
                        copyOptions = copyOptions + ' \'auto\' \n';
                    }
                } else if (config.dataFormat.S === 'PARQUET' || config.dataFormat.S === 'ORC') {
                    copyOptions = copyOptions + ' format as ' + config.dataFormat.S;
                } else {
                    callback(null, {
                        status: ERROR,
                        error: 'Unsupported data format ' + config.dataFormat.S,
                        cluster: clusterInfo.clusterEndpoint.S
                    });
                }

                // add compression directives
                if (config.compression) {
                    copyOptions = copyOptions + ' ' + config.compression.S + '\n';
                }

                // add copy options
                if (config.copyOptions !== undefined) {
                    copyOptions = copyOptions + config.copyOptions.S + '\n';
                }

                // add the encryption option to the copy command, and the master
                // symmetric key clause to the credentials
                if (config.masterSymmetricKey) {
                    copyOptions = copyOptions + "encrypted\n";

                    if (decryptedConfigItems[symmetricKeyMapEntry]) {
                        credentials = credentials + ";master_symmetric_key=" + decryptedConfigItems[symmetricKeyMapEntry].toString();
                    } else {
                        // we didn't get a decrypted symmetric key back so fail
                        callback(null, {
                            status: ERROR,
                            error: "KMS did not return a Decrypted Master Symmetric Key Value from: " + config.masterSymmetricKey.S,
                            cluster: clusterInfo.clusterEndpoint.S
                        });
                    }
                }

                // build the final copy command
                copyCommand = copyCommand + " with credentials as \'" + credentials + "\' " + copyOptions + ";\n";

                // if the post-sql option is set, insert it into the copyCommand
                if (clusterInfo.postsql && clusterInfo.postsql.S) {
                    copyCommand += clusterInfo.postsql.S + (clusterInfo.postsql.S.slice(-1) == ";" ? "" : ";") + '\n'
                }

                copyCommand += 'commit;';

                logger.debug("Copy Command Assembly Complete");
                logger.debug(copyCommand);

                // build the connection string
                var dbString = 'postgres://' + clusterInfo.connectUser.S + ":" + encodeURIComponent(decryptedConfigItems[passwordKeyMapEntry].toString()) + "@" + clusterInfo.clusterEndpoint.S + ":"
                    + clusterInfo.clusterPort.N;
                if (clusterInfo.clusterDB) {
                    dbString = dbString + '/' + clusterInfo.clusterDB.S;
                }
                if (clusterInfo.useSSL && clusterInfo.useSSL.BOOL === true) {
                    dbString = dbString + '?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory';
                }

                let overrideDbString = process.env['_OVERRIDE_DBSTRING'];
                if (overrideDbString && overrideDbString !== null) {
                    dbString = overrideDbString;
                    logger.info("Using Override Database String: " + overrideDbString);
                } else {
                    logger.info("Connecting to Database " + clusterInfo.clusterEndpoint.S + ":" + clusterInfo.clusterPort.N);
                }

                /*
				 * connect to database and run the copy command
				 */
                const pgClient = new Client({
                    connectionString: dbString
                });

                pgClient.connect((err) => {
                    if (err) {
                        logger.error(err);

                        callback(null, {
                            status: ERROR,
                            error: err,
                            cluster: clusterInfo.clusterEndpoint.S
                        });
                    } else {
                        /*
						 * run the copy command. We will allow 5 retries when
						 * the 'specified key does not exist' error is
						 * encountered, as this means an issue with eventual
						 * consistency in US Std. We will use an exponential
						 * backoff from 30ms with 5 retries - giving a max retry
						 * duration of ~ 1 second
						 */
                        runPgCommand(clusterInfo, pgClient, copyCommand, 5, ["S3ServiceException:The specified key does not exist.,Status 404"], 30, callback);
                    }
                });
            }
        });
    };

    /**
     * Function which marks a batch as failed and sends notifications
     * accordingly
     */
    function failBatch(loadState, config, thisBatchId, s3Info, manifestInfo) {
        logger.info("Failing Batch " + thisBatchId + " due to " + JSON.stringify(loadState));

        if (config.failedManifestKey && manifestInfo) {
            // copy the manifest to the failed location
            manifestInfo.failedManifestPrefix = manifestInfo.manifestPrefix.replace(manifestInfo.manifestKey + '/', config.failedManifestKey.S + '/');
            manifestInfo.failedManifestPath = manifestInfo.manifestBucket + '/' + manifestInfo.failedManifestPrefix;

            var copySpec = {
                Bucket: manifestInfo.manifestBucket,
                Key: manifestInfo.failedManifestPrefix,
                CopySource: manifestInfo.manifestPath,
                Metadata: {
                    'x-amz-meta-load-date': common.readableTime(common.now())
                }
            };

            logger.debug("Moving manifest file to failure manifest prefix");
            logger.debug(JSON.stringify(copySpec));

            s3.copyObject(copySpec, function (err, data) {
                if (err) {
                    logger.error(err);
                    closeBatch(err, config, thisBatchId, s3Info, manifestInfo);
                } else {
                    logger.info('Created new Failed Manifest ' + manifestInfo.failedManifestPath);

                    // update the batch entry showing the failed
                    // manifest location
                    var manifestModification = {
                        Key: {
                            batchId: {
                                S: thisBatchId
                            },
                            s3Prefix: {
                                S: s3Info.prefix
                            }
                        },
                        TableName: batchTable,
                        AttributeUpdates: {
                            manifestFile: {
                                Action: 'PUT',
                                Value: {
                                    S: manifestInfo.failedManifestPath
                                }
                            },
                            lastUpdate: {
                                Action: 'PUT',
                                Value: {
                                    N: '' + common.now()
                                }
                            },
                            ttl:  {
                                Action: 'PUT',
                                Value: {
                                    N: '' + Math.floor(Date.now() / 1000) + 60 * 60
                                }
                            }
                        }
                    };

                    logger.debug("Marking new failure manifest location on Batch entry");
                    logger.debug(JSON.stringify(manifestModification));

                    common.retryableUpdate(dynamoDB, manifestModification, function (err, data) {
                        if (err) {
                            console.log(err);
                            // add this new error to the original failed load
                            // state
                            closeBatch(loadState + " " + err, config, thisBatchId, s3Info, manifestInfo);
                        } else {
                            // close the batch with the original
                            // calling error
                            closeBatch(loadState, config, thisBatchId, s3Info, manifestInfo);
                        }
                    });
                }
            });
        } else {
            logger.info('Not requesting copy of Manifest to Failed S3 Location');
            closeBatch(loadState, config, thisBatchId, s3Info, manifestInfo);
        }
    };

    /**
     * Function which closes the batch to mark it as done, including
     * notifications
     */
    function closeBatch(batchError, config, thisBatchId, s3Info, manifestInfo) {
        var item = {
            Key: {
                batchId: {
                    S: thisBatchId
                },
                s3Prefix: {
                    S: s3Info.prefix
                }
            },
            TableName: batchTable,
            AttributeUpdates: {
                status: {
                    Action: 'PUT',
                    Value: {
                        S: complete
                    }
                },
                lastUpdate: {
                    Action: 'PUT',
                    Value: {
                        N: '' + common.now()
                    }
                },
                ttl:  {
                    Action: 'PUT',
                    Value: {
                        N: '' + Math.floor(Date.now() / 1000) + 60 * 60
                    }
                }
            }
        };

        // add the error message to the updates if we had one
        if (batchError && batchError !== null) {
            item.AttributeUpdates.errorMessage = {
                Action: 'PUT',
                Value: {
                    S: JSON.stringify(batchError)
                }
            };

            item.AttributeUpdates.status = {
                Action: 'PUT',
                Value: {
                    S: error
                }
            };
        }

        logger.debug("Marking Batch entry as completed");
        logger.debug(JSON.stringify(item));

        // mark the batch as closed
        common.retryableUpdate(dynamoDB, item, function (err, data) {
            // ugh, the batch closure didn't finish - this is not a good
            // place to be
            if (err) {
                logger.error("Batch closure failed. Batch will remain in load state and must be manually reset. Check whether database load completed before moving to final state.")
                logger.error(JSON.stringify(err));
                context.done(error, JSON.stringify(err));
            } else {
                // send notifications
                notify(config, thisBatchId, s3Info, manifestInfo, batchError, function (err) {
                    if (err) {
                        logger.error(JSON.stringify(err));
                        context.done(error, JSON.stringify(err) + " " + JSON.stringify(batchError));
                    } else if (batchError) {
                        logger.error(JSON.stringify(batchError));

                        // allow for an environment variable to suppress failure
                        // end status if failure notifications were correctly
                        // sent
                        if (config.failureTopicARN && process.env[SUPPRESS_FAILURE_ON_OK_NOTIFICATION] === 'true') {
                            logger.info("Suppressing failed end state due to environment setting " + SUPPRESS_FAILURE_ON_OK_NOTIFICATION);
                            context.done(null, null);
                        } else {
                            context.done(error, JSON.stringify(batchError));
                        }
                    } else {
                        logger.info("Batch Load " + thisBatchId + " Complete");
                        context.done(null, null)
                    }
                });
            }
        });
    };

    /** send an SNS message to a topic */
    function sendSNS(topic, subj, msg, callback) {
        var m = {
            Message: JSON.stringify(msg),
            Subject: subj,
            TopicArn: topic
        };

        logger.debug(`Sending SNS Notification to ${topic}`);
        logger.debug(JSON.stringify(m));

        sns.publish(m, function (err, data) {
            callback(err);
        });
    };

    /** Send SNS notifications if configured for OK vs Failed status */
    function notify(config, thisBatchId, s3Info, manifestInfo, batchError, callback) {
        var statusMessage = batchError ? 'error' : 'ok';
        var errorMessage = batchError ? JSON.stringify(batchError) : null;
        var messageBody = {
            error: errorMessage,
            status: statusMessage,
            batchId: thisBatchId,
            s3Prefix: s3Info.prefix,
            key: s3Info.key
        };

        if (manifestInfo) {
            messageBody.originalManifest = manifestInfo.manifestPath;
            messageBody.failedManifest = manifestInfo.failedManifestPath;
        }

        var sendNotifications = [];

        if (batchError && batchError !== null) {
            logger.error(JSON.stringify(batchError));

            if (config.failureTopicARN) {
                sendNotifications.push(sendSNS.bind(undefined, config.failureTopicARN.S, "Lambda Redshift Batch Load " + thisBatchId + " Failure", messageBody));
            }
        }

        if (config.successTopicARN) {
            sendNotifications.push(sendSNS.bind(undefined, config.successTopicARN.S, "Lambda Redshift Batch Load " + thisBatchId + " OK", messageBody));
        }

        async.waterfall(sendNotifications, function (err) {
            callback(err);
        });
    }

    /* end of runtime functions */

    try {
        logger.debug(JSON.stringify(event));

        if (!event.Records) {
            // filter out unsupported events
            logger.error("Event type unsupported by Lambda Redshift Loader");
            logger.info(JSON.stringify(event));
            context.done(null, null);
        } else {
            if (event.Records.length > 1) {
                context.done(error, "Unable to process multi-record events");
            } else {
                var r = event.Records[0];

                // ensure that we can process this event based on a variety
                // of criteria
                var noProcessReason;
                if (r.eventSource !== "aws:s3") {
                    noProcessReason = "Invalid Event Source " + r.eventSource;
                }
                if (!(r.eventName === "ObjectCreated:Copy" || r.eventName === "ObjectCreated:Put" || r.eventName === 'ObjectCreated:CompleteMultipartUpload')) {
                    noProcessReason = "Invalid Event Name " + r.eventName;
                }
                if (r.s3.s3SchemaVersion !== "1.0") {
                    noProcessReason = "Unknown S3 Schema Version " + r.s3.s3SchemaVersion;
                }

                if (noProcessReason) {
                    logger.error(noProcessReason);
                    context.done(error, noProcessReason);
                } else {
                    // extract the s3 details from the event
                    var inputInfo = {
                        bucket: undefined,
                        key: undefined,
                        prefix: undefined,
                        inputFilename: undefined
                    };

                    inputInfo.bucket = r.s3.bucket.name;
                    inputInfo.key = decodeURIComponent(r.s3.object.key);

                    // remove the bucket name from the key, if we have
                    // received it - this happens on object copy
                    inputInfo.key = inputInfo.key.replace(inputInfo.bucket + "/", "");

                    var keyComponents = inputInfo.key.split('/');
                    inputInfo.inputFilename = keyComponents[keyComponents.length - 1];

                    // remove the filename from the prefix value
                    var searchKey = inputInfo.key.replace(inputInfo.inputFilename, '').replace(/\/$/, '');

                    // transform hive style dynamic prefixes into static
                    // match prefixes and set the prefix in inputInfo
                    inputInfo.prefix = inputInfo.bucket + '/' + searchKey.transformHiveStylePrefix();

                    // add the object size to inputInfo
                    inputInfo.size = r.s3.object.size;

                    resolveConfig(inputInfo.prefix, function (err, configData) {
                        /*
						 * we did get a configuration found by the resolveConfig
						 * method
						 */
                        if (err) {
                            logger.error(JSON.stringify(err));
                            context.done(err, JSON.stringify(err));
                        } else {
                            // update the inputInfo prefix to match the
                            // resolved
                            // config entry
                            inputInfo.prefix = configData.Item.s3Prefix.S;

                            logger.debug(JSON.stringify(inputInfo));

                            // call the foundConfig method with the data
                            // item
                            foundConfig(inputInfo, null, configData);
                        }
                    }, function (err) {
                        // finish with no exception - where this file sits
                        // in the S3 structure is not configured for redshift
                        // loads, or there was an access issue that prevented us
                        // querying DDB
                        logger.error("No Configuration Found for " + inputInfo.prefix);
                        if (err) {
                            logger.error(err);
                        }

                        context.done(err, JSON.stringify(err));
                    });
                }

            }
        }
    } catch (e) {
        logger.error("Unhandled Exception");
        logger.error(JSON.stringify(e));
        logger.error(JSON.stringify(event));
        context.done(error, JSON.stringify(e));
    }
}

exports.handler = handler;
