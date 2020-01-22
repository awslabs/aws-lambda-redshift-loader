/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

var async = require('async');
var uuid = require('uuid');
require('./constants');
var pjson = require('./package.json');

// function which creates a string representation of now suitable for use in S3
// paths
function getFormattedDate(date) {
    if (!date) {
        date = new Date();
    }

    var hour = date.getHours();
    hour = (hour < 10 ? "0" : "") + hour;

    var min = date.getMinutes();
    min = (min < 10 ? "0" : "") + min;

    var sec = date.getSeconds();
    sec = (sec < 10 ? "0" : "") + sec;

    var year = date.getFullYear();

    var month = date.getMonth() + 1;
    month = (month < 10 ? "0" : "") + month;

    var day = date.getDate();
    day = (day < 10 ? "0" : "") + day;

    return year + "-" + month + "-" + day + " " + hour + ":" + min + ":" + sec;
};
exports.getFormattedDate = getFormattedDate;

/* current time as seconds */
function now() {
    return new Date().getTime() / 1000;
};
exports.now = now;

function readableTime(epochSeconds) {
    var d = new Date(0);
    d.setUTCSeconds(epochSeconds);
    return getFormattedDate(d);
};
exports.readableTime = readableTime;

function createTableAndWait (tableParams, dynamoDB, callback) {
    dynamoDB.createTable(tableParams, function (err, data) {
        if (err) {
            if (err.code !== 'ResourceInUseException') {
                console.log(err.toString());
                callback(err);
            } else {
                console.log("Table " + tableParams.TableName + " already exists");
                callback();
            }
        } else {
            console.log("Created DynamoDB Table " + tableParams.TableName);
            setTimeout(callback, 1000);
        }
    });
};
exports.createTableAndWait = createTableAndWait;

function createTables (dynamoDB, callback) {
    // processed files table spec
    var pfKey = 'loadFile';
    var processedFilesSpec = {
        AttributeDefinitions: [{
            AttributeName: pfKey,
            AttributeType: 'S'
        }],
        KeySchema: [{
            AttributeName: pfKey,
            KeyType: 'HASH'
        }],
        TableName: filesTable,
        ProvisionedThroughput: {
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 5
        }
    };
    var configKey = s3prefix;
    var configSpec = {
        AttributeDefinitions: [{
            AttributeName: configKey,
            AttributeType: 'S'
        }],
        KeySchema: [{
            AttributeName: configKey,
            KeyType: 'HASH'
        }],
        TableName: configTable,
        ProvisionedThroughput: {
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 5
        }
    };

    var batchKey = batchId;
    var inputLoc = s3prefix;
    var batchSpec = {
        AttributeDefinitions: [{
            AttributeName: batchKey,
            AttributeType: 'S'
        }, {
            AttributeName: 'status',
            AttributeType: 'S'
        }, {
            AttributeName: lastUpdate,
            AttributeType: 'N'
        }, {
            AttributeName: inputLoc,
            AttributeType: 'S'
        }],
        KeySchema: [{
            AttributeName: inputLoc,
            KeyType: 'HASH'
        }, {
            AttributeName: batchKey,
            KeyType: 'RANGE'
        }],
        TableName: batchTable,
        ProvisionedThroughput: {
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 5
        },
        GlobalSecondaryIndexes: [{
            IndexName: batchStatusGSI,
            KeySchema: [{
                AttributeName: 'status',
                KeyType: 'HASH'
            }, {
                AttributeName: lastUpdate,
                KeyType: 'RANGE'
            }],
            Projection: {
                ProjectionType: 'ALL'
            },
            ProvisionedThroughput: {
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 5
            }
        }]
    };

    let functions = [
    	createTableAndWait.bind(undefined, processedFilesSpec, dynamoDB), 
    	createTableAndWait.bind(undefined, batchSpec, dynamoDB),
        createTableAndWait.bind(undefined, configSpec, dynamoDB)
    ];
    async.waterfall(functions, function (err, results) {
        if (err) {
            console.log(err);
            callback(err);
        } else {
            callback();
        }
    });
};
exports.createTables = createTables;

function retryableUpdate(dynamoDB, updateRequest, callback) {
    var tryNumber = 0;
    var writeRetryLimit = 100;
    var done = false;

    async.whilst(function (test_cb) {
        // retry until the try count is hit
        test_cb(null, tryNumber < writeRetryLimit && done === false);
    }, function (asyncCallback) {
        tryNumber++;

        dynamoDB.updateItem(updateRequest, function (err, data) {
            if (err) {
                if (err.code === 'ResourceInUseException' || err.code === 'ResourceNotFoundException' || err.code === 'ProvisionedThroughputExceededException') {
                    // retry in 1 second if the table is still in the process of
                    // being created
                    setTimeout(asyncCallback, 1000);
                } else {
                    console.log(JSON.stringify(updateRequest));
                    console.log(err);
                    asyncCallback(err);
                }
            } else {
                // all OK - exit OK
                if (data) {
                    done = true;
                    asyncCallback(undefined, data);
                } else {
                    var msg = "Wrote to DynamoDB but didn't receive a verification data element";
                    console.log(msg);
                    asyncCallback(msg);
                }
            }
        });
    }, function (err, data) {
    	console.log(err);
        callback(err, data);
    });
};
exports.retryableUpdate = retryableUpdate;

function retryablePut (dynamoDB, putRequest, callback) {
    var tryNumber = 0;
    var writeRetryLimit = 100;
    var done = false;

    async.whilst(function (test_cb) {
        // retry until the try count is hit
        test_cb(null, tryNumber < writeRetryLimit && done === false);
    }, function (asyncCallback) {
        tryNumber++;
        dynamoDB.putItem(putRequest, function (err, data) {
            if (err) {
                if (err.code === 'ResourceInUseException' || err.code === 'ResourceNotFoundException' || err.code === 'ProvisionedThroughputExceededException') {
                    // retry in 1 second if the table is still in the process of
                    // being created
                    setTimeout(asyncCallback, 1000);
                } else {
                    console.log(JSON.stringify(putRequest));
                    console.log(err);
                    done=true;
                    asyncCallback(err);
                }
            } else {
                // all OK - exit OK
                if (data) {
                    done = true;
                    asyncCallback(undefined, data);
                } else {
                    var msg = "Wrote to DynamoDB but didn't receive a verification data element";
                    console.log(msg);
                    done=true;
                    asyncCallback(msg);
                }
            }
        });
    }, function (err) {
        callback(err);
    });
};
exports.retryablePut = retryablePut;

function dropTables(dynamoDB, callback) {
    // drop the config table
    dynamoDB.deleteTable({
        TableName: configTable
    }, function (err, data) {
        if (err && err.code !== 'ResourceNotFoundException') {
            console.log(err);
            callback(err);
        } else {
            // drop the processed files table
            dynamoDB.deleteTable({
                TableName: filesTable
            }, function (err, data) {
                if (err && err.code !== 'ResourceNotFoundException') {
                    console.log(err);
                    callback(err);
                } else {
                    // drop the batches table
                    dynamoDB.deleteTable({
                        TableName: batchTable
                    }, function (err, data) {
                        if (err && err.code !== 'ResourceNotFoundException') {
                            console.log(err);
                            callback(err);
                        }

                        console.log("All Configuration Tables Dropped");

                        // call the callback requested
                        if (callback) {
                            callback();
                        }
                    });
                }
            });
        }
    });
};
exports.dropTables = dropTables;

/* validate that the given value is a number, and if so return it */
function getIntValue(value, rl) {
    if (!value || value === null) {
        rl.close();
        console.log('Null Value');
        process.exit(INVALID_ARG);
    } else {
        var num = parseInt(value);

        if (isNaN(num)) {
            rl.close();
            console.log('Value \'' + value + '\' is not a Number');
            process.exit(INVALID_ARG);
        } else {
            return num;
        }
    }
};
exports.getIntValue = getIntValue;

function getBooleanValue(value) {
    if (value) {
        if (['TRUE', '1', 'YES', 'Y'].indexOf(value.toUpperCase()) > -1) {
            return true;
        } else {
            return false;
        }
    } else {
        return false;
    }
};
exports.getBooleanValue = getBooleanValue;

/* validate that the provided value is not null/undefined */
function validateNotNull(value, message, rl) {
    if (!value || value === null || value === '') {
        rl.close();
        console.log(message);
        process.exit(INVALID_ARG);
    }
};
exports.validateNotNull = validateNotNull;

/* turn blank lines read from STDIN to Null */
function blank(value) {
    if (!value || value === '') {
        return null;
    } else {
        return value;
    }
};
exports.blank = blank;

function validateArrayContains(array, value, rl) {
    if (array.indexOf(value) === -1) {
        rl.close();
        console.log('Value must be one of ' + array.toString());
        process.exit(INVALID_ARG);
    }
};
exports.validateArrayContains = validateArrayContains;


function createManifestInfo(config) {
    // manifest file will be at the configuration location, with a fixed
    // prefix and the date plus a random value for uniqueness across all
    // executing functions
    var dateName = getFormattedDate();
    var rand = Math.floor(Math.random() * 10000);

    var manifestInfo = {
        manifestBucket: config.manifestBucket.S,
        manifestKey: config.manifestKey.S,
        manifestName: 'manifest-' + dateName + '-' + rand
    };
    manifestInfo.manifestPrefix = manifestInfo.manifestKey + '/' + manifestInfo.manifestName;
    manifestInfo.manifestPath = manifestInfo.manifestBucket + "/" + manifestInfo.manifestPrefix;

    return manifestInfo;
};
exports.createManifestInfo = createManifestInfo;

function randomInt(low, high) {
    return Math.floor(Math.random() * (high - low) + low);
};
exports.randomInt = randomInt;

function getFunctionArn(lambda, functionName, callback) {
    var params = {
        FunctionName: functionName
    };
    lambda.getFunction(params, function (err, data) {
        if (err) {
            console.log(err);
            callback(err);
        } else {
            if (data && data.Configuration) {
                callback(undefined, data.Configuration.FunctionArn);
            } else {
                callback();
            }
        }
    });
};
exports.getFunctionArn = getFunctionArn;

function getS3NotificationConfiguration(s3, bucket, prefix, functionArn, callback) {
    var params = {
        Bucket: bucket
    };
    s3.getBucketNotificationConfiguration(params, function (err, data) {
        if (err) {
            callback(err);
        } else {
            // have to iterate through all the function configurations
            if (data.LambdaFunctionConfigurations && data.LambdaFunctionConfigurations.length > 0) {
                var matchConfigId;
                data.LambdaFunctionConfigurations.map(function (item) {
                    if (item && item.Filter && item.Filter.Key && item.Filter.Key.FilterRules) {
                        item.Filter.Key.FilterRules.map(function (filter) {
                            if (filter.Name === 'Prefix' && filter.Value === prefix) {
                                if (item.LambdaFunctionArn === functionArn) {
                                    matchConfigId = item.Id;
                                }
                            }
                        });
                    }
                });

                if (matchConfigId) {
                    callback(undefined, matchConfigId, data);
                } else {
                    callback(undefined, undefined, data);
                }
            } else {
                callback();
            }
        }
    });
};
exports.getS3NotificationConfiguration = getS3NotificationConfiguration;

function getS3Arn(bucket, prefix) {
    var arn = "arn:aws:s3:::" + bucket;

    if (prefix) {
        arn = arn + prefix;
    }

    return arn;
}
exports.getS3Arn = getS3Arn;

function ensureS3InvokePermisssions(lambda, bucket, prefix, functionName, functionArn, callback) {
    lambda.getPolicy({
        FunctionName: functionName
    }, function (err, data) {
        if (err && err.code !== 'ResourceNotFoundException') {
            callback(err);
        }

        var foundMatch = false;
        var s3Arn = getS3Arn(bucket);
        var sourceAccount = functionArn.split(":")[4];

        // process the existing permissions policy if there is one
        if (data && data.Policy) {
            var statements = JSON.parse(data.Policy).Statement;

            statements.map(function (item) {
                try {
                    if (item.Resource === functionArn && item.Condition.StringEquals['AWS:SourceAccount'] === sourceAccount) {
                        foundMatch = true;
                    }
                } catch (e) {
                    // this is OK - just means that the policy structure doesn't
					// match the above format

                }
            });
        }

        if (foundMatch === true) {
            console.log("Found existing Policy match for S3 path to invoke " + functionName);
            callback();
        } else {
            var lambdaPermissions = {
                Action: "lambda:InvokeFunction",
                FunctionName: functionName,
                Principal: "s3.amazonaws.com",
                // only use internal account sources
                SourceAccount: sourceAccount,
                SourceArn: s3Arn,
                StatementId: uuid.v4()
            };

            lambda.addPermission(lambdaPermissions, function (err, data) {
                if (err) {
                    console.log(err);
                    callback(err);
                } else {
                    console.log("Granted S3 permission to invoke " + functionArn);
                    callback();
                }
            });
        }
    });
}
exports.ensureS3InvokePermisssions = ensureS3InvokePermisssions;

function createS3EventSource (s3, lambda, bucket, prefix, functionName, callback) {
    console.log("Creating S3 Event Source for s3://" + bucket + "/" + prefix);

    // lookup the deployed function name to get the ARN
    getFunctionArn(lambda, functionName, function (err, functionArn) {
        if (err) {
        	console.log(err);
            callback(err);
        } else {
            // blow up if there's no deployed function - can't create the event
            // source
            if (!functionArn) {
                var msg = "Unable to resolve Function ARN for " + functionName;
                console.log(msg);
                callback(msg);
            } else {
                getS3NotificationConfiguration(s3, bucket, prefix, functionArn, function (err, lambdaFunctionId, currentNotificationConfiguration) {
                    if (err) {
                        // this almost certainly will be because the bucket name
                        // doesn't exist
                        console.log(err);
                        callback(err);
                    } else {
                        if (lambdaFunctionId) {
                            // found an existing function
                            console.log("Found existing event source for s3://" + bucket + "/" + prefix + " forwarding notifications to " + functionArn);
                            callback(undefined, lambdaFunctionId);
                        } else {
                            // there isn't a matching event
                            // configuration so create a new one for the
                            // specified prefix
                            ensureS3InvokePermisssions(lambda, bucket, prefix, functionName, functionArn, function (err, data) {
                                if (err) {
                                    callback(err);
                                } else {
                                    // add the notification configuration to the
                                    // set of existing lambda configurations
                                    if (!currentNotificationConfiguration) {
                                        currentNotificationConfiguration = {};
                                    }
                                    if (!currentNotificationConfiguration.LambdaFunctionConfigurations) {
                                        currentNotificationConfiguration.LambdaFunctionConfigurations = [];
                                    }

                                    configAlreadyExists = false;

                                    // Let's check our configs to see if we
									// already have one that exists
                                    currentNotificationConfiguration.LambdaFunctionConfigurations.forEach(config => {
                                        if (config.Filter.Key.FilterRules[0].Value == prefix + '/') {
                                            console.log('Skipping creation of notification config because it already exists');
                                            configAlreadyExists = true;
                                        }
                                    });

                                    // Create a new notification config
                                    if (!configAlreadyExists) {
                                        console.log('Creating notification configuration');

                                        // now create the event source mapping
                                        var newEventConfiguration = {
                                            Events: ['s3:ObjectCreated:*',],
                                            LambdaFunctionArn: functionArn,
                                            Filter: {
                                                Key: {
                                                    FilterRules: [{
                                                        Name: 'prefix',
                                                        Value: prefix + "/"
                                                    }]
                                                }
                                            },
                                            Id: "LambdaRedshiftLoaderEventSource-" + uuid.v4()
                                        };

                                        currentNotificationConfiguration.LambdaFunctionConfigurations.push(newEventConfiguration);

                                        // push the function event trigger
                                        // configurations back into S3
                                        var params = {
                                            Bucket: bucket,
                                            NotificationConfiguration: currentNotificationConfiguration
                                        };

                                        s3.putBucketNotificationConfiguration(params, function (err, data) {
                                            if (err) {
                                                console.log(this.httpResponse.body.toString());
                                                console.log(err);
                                                callback(err);
                                            } else {
                                                callback();
                                            }
                                        });
                                    }
                                }
                            });
                        }
                    }
                });
            }
        }
    });
};
exports.createS3EventSource = createS3EventSource;

// function which sets up the tables, writes the configuration, and creates the
// event source for S3->Lambda
function setup (useConfig, dynamoDB, s3, lambda, callback) {
    // function to create tables
    var ct = function (c) {
    	console.log("Creating required configuration tables in DynamoDB")
        createTables(dynamoDB, function (err) {
            c(err);
        });
    };

    // function to write the configuration into the config tables
    var wc = function (c) {
    	console.log("Creating Configuration");
    	retryablePut(dynamoDB, useConfig, function (err) {
            c(err);
        });
    };

    // function which invokes the creation of the event source for the bucket
    // and prefix
    var ces = function (c) {
    	console.log("Creating S3 Event Source")
        var s3prefix = useConfig.Item.s3Prefix.S;
        var tokens = s3prefix.split("/");
        var bucket = tokens[0];
        var prefix = tokens.slice(1).join("/");

        // deployedFunctionName is defined in constants.js
        createS3EventSource(s3, lambda, bucket, prefix, deployedFunctionName, function (err, configId) {
            c(err);
        });
    };

    async.waterfall([ct, wc, ces], function (err, result) {
        if (err) {
            console.log(err);
            callback(err);
        } else {
            callback();
        }
    });
};
exports.setup = setup;

function inPlaceCopyFile(s3, batchId, batchEntry, callback) {
    // issue a same source/target copy command to S3, which will cause
    // Lambda to receive a new event
    var bucketName = batchEntry.split("/")[0];
    var fileKey = batchEntry.replace(bucketName + "\/", "");
    var headSpec = {
        Bucket: bucketName,
        Key: fileKey,
    };
    s3.headObject(headSpec, function (err, data) {
        if (err) {
            console.log(err);
            callback(err);
        } else {
            // Modify the metadata to allow the in-place copy
            var meta;
            if (data.Metadata) {
                meta = data.Metadata;
            } else {
                meta = {}
            }

            if (batchId) {
                meta["x-amz-meta-copy-reason"] = "AWS Lambda Redshift Loader Reprocess Batch " + batchId;
            } else {
                meta["x-amz-meta-copy-reason"] = "AWS Lambda Redshift Loader Reprocess File";
            }

            // request the copy
            var copySpec = {
                Metadata: meta,
                MetadataDirective: "REPLACE",
                Bucket: bucketName,
                Key: fileKey,
                CopySource: batchEntry
            };
            s3.copyObject(copySpec, function (err, data) {
                if (err) {
                    console.log(err);
                    callback(err);
                } else {
                    console.log("Submitted reprocess request for " + batchEntry);

                    // done - call the callback
                    callback();
                }
            });
        }
    });
}
exports.inPlaceCopyFile = inPlaceCopyFile;

function updateConfig(s3Prefix, configAttribute, configValue, dynamoDB, callback) {
    var dynamoConfig = {
        TableName: configTable,
        Key: {
            "s3Prefix": {
                S: s3Prefix
            }
        },
        ExpressionAttributeNames: {
            "#attribute": configAttribute
        }
    };

    if (configValue) {
        dynamoConfig.UpdateExpression = "set #attribute = :value, #version = :version";
        dynamoConfig.ExpressionAttributeValues = {
            ":value":
                {
                    S: configValue
                }
        };
        dynamoConfig.ExpressionAttributeNames["#version"] = "version";
        dynamoConfig.ExpressionAttributeValues[":version"] = {
            S: pjson.version
        };
    } else {
        dynamoConfig.UpdateExpression = "remove #attribute";
    }

    retryableUpdate(dynamoDB, dynamoConfig, function (err, data) {
        callback(err);
    });
}
exports.updateConfig = updateConfig;

function deleteFile(dynamoDB, region, file, callback) {
    var fileItem = {
        Key: {
            loadFile: {
                S: file
            }
        },
        TableName: filesTable
    };

    dynamoDB.deleteItem(fileItem, function (err, data) {
        callback(err, data);
    });
}
exports.deleteFile = deleteFile;

function queryFile(dynamoDB, region, file, callback) {
    var fileItem = {
        Key: {
            loadFile: {
                S: file
            }
        },
        TableName: filesTable
    };

    dynamoDB.getItem(fileItem, function (err, data) {
        callback(err, data);
    });
}
exports.queryFile = queryFile;

function reprocessFile(dynamoDB, s3, region, file, callback) {
    // get the file so we know what the current batch ID is
    var fileItem = {
        Key: {
            loadFile: {
                S: file
            }
        },
        TableName: filesTable
    };
    dynamoDB.getItem(fileItem, function (err, data) {
        if (err) {
            if (callback) {
                callback(err);
            }
        } else {
            if (data.Item.batchId && data.Item.batchId.S) {
                var updateExpr = "remove #batchId ";

                if (data.Item.previousBatches) {
                    // add to the end of the list
                    updateExpr = updateExpr + "set previousBatches = list_append(previousBatches,:oldBatch)"
                } else {
                    // create a new list
                    updateExpr = updateExpr + "set previousBatches = :oldBatch";
                }

                // rotate the current batch information onto a tracking list
                var update = {
                    Key: {
                        loadFile: {
                            S: file
                        }
                    },
                    TableName: filesTable,
                    ExpressionAttributeNames: {
                        "#batchId": "batchId"
                    },
                    ExpressionAttributeValues: {
                        ":oldBatch": {
                            L: [
                                {S: "" + data.Item.batchId.S}
                            ]
                        }
                    },
                    UpdateExpression: updateExpr,
                    ReturnValues: "ALL_NEW"
                };

                dynamoDB.updateItem(update, function (err, data) {
                    if (err) {
                        callback(err);
                    } else {
                        // now the file needs an in-place copy with new metadata
						// to cause a reprocess
                        inPlaceCopyFile(s3, undefined, file, function (err, data) {
                            if (callback) {
                                callback(err);
                            }
                        });
                    }
                });
            } else {
                // not currently assigned to a batch, so just do an s3 update
                inPlaceCopyFile(s3, undefined, file, function (err, data) {
                    if (callback) {
                        callback(err);
                    }
                });
            }
        }
    });
}
exports.reprocessFile = reprocessFile;