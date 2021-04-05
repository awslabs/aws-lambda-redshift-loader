var aws = require('aws-sdk');
require('./constants');
var common = require('./common');
var async = require('async');
var debug = true;
var dynamoDB;
var s3;
var debug = (process.env['DEBUG'] === 'true');
var log_level = process.env['LOG_LEVEL'] || 'info';
const winston = require('winston');

const logger = winston.createLogger({
  level: debug === true ? 'debug' : log_level,
  transports: [
    new winston.transports.Console({
        format: winston.format.simple()
    })
  ]
});

/**
 * Initialisation for the module - connect to DDB etc
 *
 * @param setRegion
 * @returns
 */
function init(setRegion) {
    // connect to dynamo if we haven't already
    if (!dynamoDB) {
        dynamoDB = new aws.DynamoDB({
            apiVersion: '2012-08-10',
            region: setRegion
        });
    }

    if (!s3) {
        // create an S3 client for the region to hand to the in-place copy processor
        s3 = new aws.S3({
            apiVersion: '2006-03-01',
            region: setRegion
        });
    }
}

/**
 * Validate that dates are given as a number
 *
 * @param forValue
 * @returns
 */
function getDate(forValue) {
    var dt = parseInt(forValue);

    if (isNaN(dt)) {
        throw new Error(forValue + " is not a valid epoch seconds value");
    } else {
        return dt;
    }
}

function getBatch(setRegion, s3Prefix, batchId, callback) {
    init(setRegion);

    var getBatch = {
        Key: {
            "batchId": {
                S: batchId,
            },
            "s3Prefix": {
                S: s3Prefix
            }
        },
        TableName: batchTable,
        ConsistentRead: true
    };

    dynamoDB.getItem(getBatch, function (err, data) {
        if (err) {
            callback(err);
        } else {
            if (data && data.Item) {
                callback(null, data.Item);
            } else {
                callback("No Batch " + batchId + " for prefix " + s3Prefix + " found in " + setRegion);
            }
        }
    });
}

exports.getBatch = getBatch;

function cleanBatches(setRegion, s3Prefix, callback) {
    init(setRegion);

    // query for batches based on given s3Prefix
    queryBatchByPrefix(setRegion, s3Prefix, function (err, data) {
        if (err) {
            callback(err);
        } else {
            async.map(data, function (batchItem, asyncCallback) {
                //clean found batches one by one
                cleanBatch(setRegion, batchItem, function (err, data) {
                    if (err) {
                        asyncCallback(err);
                    } else {
                        asyncCallback(null, data);
                    }
                });
            }, function (err, results) {
                if (err) {
                    callback(err);
                } else {
                    // deletions are completed
                    callback(null, {
                        batchCountDeleted: results.length,
                        batchesDeleted: results
                    });
                }
            });

        }
    });
}

function cleanBatch(setRegion, batchItem, callback) {
    // delete batch entry
    deleteBatch(batchItem.s3Prefix, batchItem.batchId, function (err, data) {
        if (err) {
            callback(err);
        } else {
            if ( !batchItem.entries || batchItem.entries.length <= 0) {
                callback(null, data);
            } else {
                //delete related  entries in filesTable
                async.map(batchItem.entries,  function (processedFile, asyncCallback) {
                    common.deleteFile(dynamoDB, setRegion, processedFile, function (err, data) {
                        if (err) {
                            asyncCallback(err);
                        } else {
                            asyncCallback(null, data);
                        }
                    });
                }, function (err, results) {
                    if (err) {
                        callback(err);
                    } else {
                        data.processedFilesCountDeleted = results.length;
                        data.processedFilesDeleted = results;
                        callback(null, data);
                    }
                });
            }
        }
    });
}

function queryBatchByPrefix(setRegion, s3Prefix, callback) {
    init(setRegion);
    var keyConditionExpression = null;
    var keyConditionNames = null;
    var keyConditionValues = null;

    queryParams = {
        TableName: batchTable
    };

    keyConditionExpression = "#s3Prefix = :s3Prefix";
    // add s3Prefix
    keyConditionNames = {
        "#s3Prefix": "s3Prefix"
    };
    keyConditionValues = {
        ":s3Prefix": {
            "S": "" + s3Prefix
        }
    };

    queryParams.KeyConditionExpression = keyConditionExpression;
    queryParams.ExpressionAttributeNames = keyConditionNames;
    queryParams.ExpressionAttributeValues = keyConditionValues;

    if (debug == true) {
        console.log(queryParams);
    }

    dynamoDB.query(queryParams, function (err, data) {
        if (err) {
            logger.error(err);
            process.exit(ERROR);
        } else {
            if (data && data.Items) {
                var itemsToShow = [];

                data.Items.map(function (item) {
                    toShow = {
                        s3Prefix: item.s3Prefix.S,
                        batchId: item.batchId.S,
                        status: item.status.S,
                        entries: item.entries.SS,
                        lastUpdateDate: common.readableTime(item.lastUpdate.N),
                        lastUpdate: item.lastUpdate.N
                    };
                    itemsToShow.push(toShow);
                });

                callback(null, itemsToShow);
            } else {
                callback(null, []);
            }
        }
    });
}

exports.cleanBatches = cleanBatches;

/**
 * Function which performs a batch query with the provided arguments
 *
 * @param setRegion
 * @param batchStatus
 * @param queryStartDate
 * @param queryEndDate
 * @param callback
 * @returns
 */
function doQuery(setRegion, batchStatus, queryStartDate, queryEndDate, callback) {
    init(setRegion);

    if (queryStartDate) {
        var startDate = getDate(queryStartDate);
    }
    if (queryEndDate) {
        var endDate = getDate(queryEndDate);
    }

    queryParams = {
        TableName: batchTable,
        IndexName: batchStatusGSI
    };

            // add the batch status
    var keyConditionExpression = "#s = :batchStatus";
    var keyConditionNames = {
                "#s": "status"
            };
    var keyConditionValues = {
                ":batchStatus": {
                    'S': batchStatus
                }
            };

    // add the start date, if provided
    if (startDate && !endDate) {
        keyConditionExpression += " and lastUpdate >= :startDate";
        keyConditionValues[":startDate"] = {
            "N": "" + startDate
        };
    } else if (!startDate && endDate) {
        keyConditionExpression += " and lastUpdate <= :endDate";
        keyConditionValues[":endDate"] = {
            "N": "" + endDate
        };
    } else if (startDate && endDate) {
        keyConditionExpression += " and lastUpdate between :startDate and :endDate";
        keyConditionValues[":startDate"] = {
            "N": "" + startDate
        };
        keyConditionValues[":endDate"] = {
            "N": "" + endDate
        };
    } // else we have neither so ignore

    // add the query expressions to the query item
    queryParams.KeyConditionExpression = keyConditionExpression;
    queryParams.ExpressionAttributeNames = keyConditionNames;
    queryParams.ExpressionAttributeValues = keyConditionValues;

    if (debug == true) {
        console.log(queryParams);
    }

    dynamoDB.query(queryParams, function (err, data) {
        if (err) {
            logger.error(err);
            process.exit(ERROR);
        } else {
            if (data && data.Items) {
                var itemsToShow = [];

                data.Items.map(function (item) {
                    toShow = {
                        s3Prefix: item.s3Prefix.S,
                        batchId: item.batchId.S,
                        status: item.status.S,
                        lastUpdateDate: common.readableTime(item.lastUpdate.N),
                        lastUpdate: item.lastUpdate.N
                    };
                    itemsToShow.push(toShow);
                });

                callback(null, itemsToShow);
            } else {
                callback(null, []);
            }
        }
    });
}

exports.doQuery = doQuery;

/**
 * Function to delete a specific batch from DynamoDB LambdaRedshiftBatches table
 *
 * @param batchId
 * @param callback
 * @returns
 */
function deleteBatch(s3Prefix, batchId, callback) {
    var deleteParams = {
        TableName: batchTable,
        Key: {
            "s3Prefix": {
                "S": s3Prefix
            },
            "batchId": {
                "S": batchId
            }
        },
        ReturnValues: 'ALL_OLD'
    }
    dynamoDB.deleteItem(deleteParams, function (err, data) {
        if (err) {
            callback(err);
        } else {
            // create the response object
            var response = {
                "lastUpdateDate": common.readableTime(data.Attributes.lastUpdate.N),
            };
            // map in all the old values that we received to the response
            Object.keys(data.Attributes).map(function (key) {
                response[key] = data.Attributes[key];
            });
            callback(null, response);
        }
    })
}

exports.deleteBatch = deleteBatch;

function deleteBatches(setRegion, s3Prefix, batchStatus, startDate, endDate, dryRun, callback) {
    init(setRegion);

    // query for batches in the range we require
    doQuery(setRegion, s3Prefix, batchStatus, startDate, endDate, function (err, data) {
        if (err) {
            callback(err);
        } else {
            if (dryRun && !JSON.parse(dryRun)) {
                logger.info("Deleting " + data.length + " Batches in status " + batchStatus);

                async.map(data, function (batchItem, asyncCallback) {
                    // pass the request through the function that deletes the
                    // item from DynamoDB
                    deleteBatch(batchItem.s3Prefix, batchItem.batchId, function (err, data) {
                        if (err) {
                            asyncCallback(err);
                        } else {
                            asyncCallback(null, data);
                        }
                    });
                }, function (err, results) {
                    if (err) {
                        callback(err);
                    } else {
                        // deletions are completed
                        callback(null, {
                            batchCountDeleted: results.length,
                            batchesDeleted: results
                        });
                    }
                });
            } else {
                logger.info("Dry run only - no batches will be modified");
                logger.info("Resolved " + data.length + " Batches for Deletion");
                callback(null, {
                    batchCountDeleted: 0,
                    batchesDeleted: data
                });
            }
        }
    });
}

exports.deleteBatches = deleteBatches;

/*
Function to reprocess an existing batch that is in a failed locked state, or an error state where the issue has been mitigated
 */
function reprocessBatch(s3Prefix, batchId, region, omitFiles, callback) {
    init(region);

    STATUS_REPROCESSING = 'reprocessing';

    getBatch(region, s3Prefix, batchId, function (err, data) {
        if (err) {
            callback(err);
        } else {
            if (data) {
                if (!data.entries.SS) {
                    msg = "Batch is Empty!";
                    logger.info(msg);
                    callback(msg);
                } else if (data.status.S === open) {
                    msg = "Cannot reprocess an Open Batch";
                    logger.error(msg);
                    callback(msg);
                } else {
                    // the batch to be unlocked must be in locked or error state - we
                    // can't reopen 'complete' batches
                    preconditionStatus = [{
                        S: 'locked'
                    }, {
                        S: 'error'
                    }];

                    // set the batch to 'reprocessing' status
                    updateBatchStatus(s3Prefix, batchId, STATUS_REPROCESSING, preconditionStatus, "Reprocessing initiated by reprocessBatch request", function (err) {
                        if (err) {
                            if (err.code === conditionCheckFailed) {
                                callback("Batch to be reprocessed must either be Locked or Error status")
                            } else {
                                callback(err);
                            }
                        } else {
                            // create a list of files which filters out the omittedFiles
                            var processFiles = [];
                            if (omitFiles) {
                                data.entries.SS.map(function (item) {
                                    if (omitFiles.indexOf(item) === -1) {
                                        // file is not in the omit list, so add it to the process list
                                        processFiles.push(item);
                                    }
                                });
                            } else {
                                processFiles = data.entries.SS;
                            }

                            // for each of the current file entries, execute the processedFiles reprocess method
                            var fileReprocessor = common.reprocessFile.bind(undefined, dynamoDB, s3, region);

                            async.map(processFiles, fileReprocessor, function (err) {
                                if (err) {
                                    callback(err);
                                } else {
                                    var preconditionStatus = [{
                                        S: STATUS_REPROCESSING
                                    }];

                                    // files have been reprocessed, so now set the batch status to reprocessed to indicate that it is closed
                                    updateBatchStatus(s3Prefix, batchId, 'reprocessed', preconditionStatus, undefined, function (err) {
                                        callback(err);
                                    });
                                }
                            });
                        }
                    });
                }
            } else {
                msg = "Unable to retrieve batch " + batchId + " for prefix " + s3Prefix;
                logger.error(msg);
                callback(msg);
            }
        }
    });
}

exports.reprocessBatch = reprocessBatch;

function updateBatchStatus(s3Prefix, thisBatchId, status, requireStatusArray, updateReason, callback) {
    var updateBatchStatus = {
        Key: {
            batchId: {
                S: thisBatchId,
            },
            s3Prefix: {
                S: s3Prefix
            }
        },
        TableName: batchTable,
        AttributeUpdates: {
            status: {
                Action: 'PUT',
                Value: {
                    S: status
                }
            },
            lastUpdate: {
                Action: 'PUT',
                Value: {
                    N: '' + common.now()
                }
            }
        }
    };

    // add the update reason if we have one
    if (updateReason) {
        updateBatchStatus.AttributeUpdates['updateReason'] = {
            Action: 'PUT',
            Value: {
                S: updateReason
            }
        };
    }

    // add preconditions and correct operator if provided
    if (requireStatusArray) {
        updateBatchStatus.Expected = {
            status: {
                AttributeValueList: requireStatusArray,
                ComparisonOperator: requireStatusArray.length > 1 ? 'IN' : 'EQ'
            }
        }
    }

    dynamoDB.updateItem(updateBatchStatus, function (err, data) {
        if (err) {
            callback(err);
        } else {
            callback();
        }
    });
};

exports.updateBatchStatus = updateBatchStatus;
