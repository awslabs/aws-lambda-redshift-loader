var aws = require('aws-sdk');
require('./constants');
var common = require('./common');
var async = require('async');
var debug = true;
var dynamoDB;

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
                callback("No Batch " + thisBatchId + " found in " + setRegion);
            }
        }
    });
}

exports.getBatch = getBatch;

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

    if (debug) {
        console.log(queryParams);
    }

    dynamoDB.query(queryParams, function (err, data) {
        if (err) {
            console.log(err);
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

function deleteBatches(setRegion, batchStatus, startDate, endDate, dryRun, callback) {
    init(setRegion);

    // query for batches in the range we require
    doQuery(setRegion, batchStatus, startDate, endDate, function (err, data) {
        if (err) {
            callback(err);
        } else {
            if (dryRun && !JSON.parse(dryRun)) {
                console.log("Deleting " + data.length + " Batches in status " + batchStatus);

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
                console.log("Dry run only - no batches will be modified");
                console.log("Resolved " + data.length + " Batches for Deletion");
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
function reprocessBatch(s3Prefix, batchId, region, callback) {
    init(region);

    STATUS_REPROCESSING = 'reprocessing';

    // create an S3 client for the region to hand to the in-place copy processor
    var s3 = new aws.S3({
        apiVersion: '2006-03-01',
        region: region
    });

    getBatch(region, s3Prefix, batchId, function (err, data) {
        if (err) {
            console.log(err);
            callback(err);
        } else {
            if (data) {
                if (!data.entries.SS) {
                    msg = "Batch is Empty!";
                    console.log(msg);
                    callback(msg);
                } else if (data.status.S === open) {
                    msg = "Cannot reprocess an Open Batch";
                    console.log(msg);
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
                            // for each of the current file entries, execute an in-place copy of the file in S3 so that the loader will pick them up again through new s3 events
                            async.map(data.entries.SS, common.inPlaceCopyFile.bind(undefined, s3, batchId), function (err) {
                                if (err) {
                                    callback(err);
                                } else {
                                    preconditionStatus = [{
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
                console.log(msg);
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