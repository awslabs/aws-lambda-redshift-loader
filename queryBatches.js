/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var aws = require('aws-sdk');
require('./constants');
var common = require('./common');

if (process.argv.length < 4) {
    console.log("You must provide an AWS Region Code, Batch Status, and optionally a start time and end time (as epoch seconds) to query from");
    process.exit(ERROR);
}
var setRegion = process.argv[2];
var batchStatus = process.argv[3];
var startDate;
var endDate;

function getDate(forValue) {
    var dt = parseInt(forValue);

    if (isNaN(dt)) {
	throw new Error(forValue + " is not a valid epoch seconds value");
    } else {
	return dt;
    }
}
if (process.argv.length > 4) {
    startDate = process.argv[4];
}
if (process.argv.length > 5) {
    endDate = process.argv[5];
}

var dynamoDB;

function doQuery(setRegion, batchStatus, queryStartDate, queryEndDate, callback) {
    if (startDate) {
	startDate = getDate(queryStartDate);
    }
    if (endDate) {
	endDate = getDate(queryEndDate);
    }
    if (!dynamoDB) {
	dynamoDB = new aws.DynamoDB({
	    apiVersion : '2012-08-10',
	    region : setRegion
	});
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

exports.doQuery(setRegion, batchStatus, startDate, endDate, function(err, data) {
    if (err) {
	console.log("Error: " + err);
	process.exit(-1);
    } else {
	console.log(JSON.stringify(data));
    }
});
