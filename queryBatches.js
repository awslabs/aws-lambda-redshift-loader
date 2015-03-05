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
	console.log("You must provide an AWS Region Code, Batch Status, and optionally a start time to query from");
	process.exit(ERROR);
}
var setRegion = process.argv[2];
var batchStatus = process.argv[3];
var startDate;

// use date parse to get a start time, using supported date format from
// javascript - us format only :(
if (process.argv.length > 4) {
	var ms = Date.parse(process.argv[4]);
	if (!isNaN(ms)) {
		startDate = ms / 1000;
	}
}

var dynamoDB = new aws.DynamoDB({
	apiVersion : '2012-08-10',
	region : setRegion
});

queryItem = {
	KeyConditions : {
		status : {
			ComparisonOperator : 'EQ',
			AttributeValueList : [ {
				S : batchStatus
			} ]
		}
	},
	TableName : batchTable,
	IndexName : batchStatusGSI
};

if (startDate) {
	queryItem.KeyConditions.lastUpdate = {
		ComparisonOperator : 'GE',
		AttributeValueList : [ {
			N : '' + startDate
		} ]
	};
}
dynamoDB.query(queryItem, function(err, data) {
	if (err) {
		console.log(err);
		process.exit(ERROR);
	} else {
		if (data && data.Items) {
			var itemsToShow = [];

			for (var i = 0; i < data.Items.length; i++) {
				toShow = {
					s3Prefix : data.Items[i].s3Prefix.S,
					batchId : data.Items[i].batchId.S,
					lastUpdateDate : common.readableTime(data.Items[i].lastUpdate.N)
				};
				itemsToShow.push(toShow);
			}

			console.log(JSON.stringify(itemsToShow));
		} else {
			console.log("Unable to query Batch Status");
		}
	}
});
