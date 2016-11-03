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
	    apiVersion : '2012-08-10',
	    region : setRegion
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
	Key : {
	    "batchId" : {
		S : batchId,
	    },
	    "s3Prefix" : {
		S : s3Prefix
	    }
	},
	TableName : batchTable,
	ConsistentRead : true
    };

    dynamoDB.getItem(getBatch, function(err, data) {
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
	TableName : batchTable,
	IndexName : batchStatusGSI
    };

    // add the batch status
    var keyConditionExpression = "#s = :batchStatus";
    var keyConditionNames = {
	"#s" : "status"
    };
    var keyConditionValues = {
	":batchStatus" : {
	    'S' : batchStatus
	}
    };

    // add the start date, if provided
    if (startDate && !endDate) {
	keyConditionExpression += " and lastUpdate >= :startDate";
	keyConditionValues[":startDate"] = {
	    "N" : "" + startDate
	};
    } else if (!startDate && endDate) {
	keyConditionExpression += " and lastUpdate <= :endDate";
	keyConditionValues[":endDate"] = {
	    "N" : "" + endDate
	};
    } else if (startDate && endDate) {
	keyConditionExpression += " and lastUpdate between :startDate and :endDate";
	keyConditionValues[":startDate"] = {
	    "N" : "" + startDate
	};
	keyConditionValues[":endDate"] = {
	    "N" : "" + endDate
	};
    } // else we have neither so ignore

    // add the query expressions to the query item
    queryParams.KeyConditionExpression = keyConditionExpression;
    queryParams.ExpressionAttributeNames = keyConditionNames;
    queryParams.ExpressionAttributeValues = keyConditionValues;

    if (debug) {
	console.log(queryParams);
    }

    dynamoDB.query(queryParams, function(err, data) {
	if (err) {
	    console.log(err);
	    process.exit(ERROR);
	} else {
	    if (data && data.Items) {
		var itemsToShow = [];

		data.Items.map(function(item) {
		    toShow = {
			s3Prefix : item.s3Prefix.S,
			batchId : item.batchId.S,
			status : item.status.S,
			lastUpdateDate : common.readableTime(item.lastUpdate.N),
			lastUpdate : item.lastUpdate.N
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
	TableName : batchTable,
	Key : {
	    "s3Prefix" : {
		"S" : s3Prefix
	    },
	    "batchId" : {
		"S" : batchId
	    }
	},
	ReturnValues : 'ALL_OLD'
    }
    dynamoDB.deleteItem(deleteParams, function(err, data) {
	if (err) {
	    callback(err);
	} else {
	    // create the response object
	    var response = {
		"lastUpdateDate" : common.readableTime(data.Attributes.lastUpdate.N),
	    };
	    // map in all the old values that we received to the response
	    Object.keys(data.Attributes).map(function(key) {
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
    doQuery(setRegion, batchStatus, startDate, endDate, function(err, data) {
	if (err) {
	    callback(err);
	} else {
	    if (dryRun && !JSON.parse(dryRun)) {
		console.log("Deleting " + data.length + " Batches in status " + batchStatus);

		async.map(data, function(batchItem, asyncCallback) {
		    // pass the request through the function that deletes the
		    // item from DynamoDB
		    deleteBatch(batchItem.s3Prefix, batchItem.batchId, function(err, data) {
			if (err) {
			    asyncCallback(err);
			} else {
			    asyncCallback(null, data);
			}
		    });
		}, function(err, results) {
		    if (err) {
			callback(err);
		    } else {
			// deletions are completed
			callback(null, {
			    batchCountDeleted : results.length,
			    batchesDeleted : results
			});
		    }
		});
	    } else {
		console.log("Dry run only - no batches will be modified");
		console.log("Resolved " + data.length + " Batches for Deletion");
		callback(null, {
		    batchCountDeleted : 0,
		    batchesDeleted : data
		});
	    }
	}
    });
}
exports.deleteBatches = deleteBatches;