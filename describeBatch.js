/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var aws = require('aws-sdk');
require('./constants');

if (process.argv.length < 4) {
	console.log("You must provide an AWS Region Code, Batch ID, and configured Input Location");
	process.exit(ERROR);
}
var setRegion = process.argv[2];
var thisBatchId = process.argv[3];
var prefix = process.argv[4];

var dynamoDB = new aws.DynamoDB({
	apiVersion : '2012-08-10',
	region : setRegion
});

var getBatch = {
	Key : {
		batchId : {
			S : thisBatchId,
		},
		s3Prefix : {
			S : prefix
		}
	},
	TableName : batchTable,
	ConsistentRead : true
};

dynamoDB.getItem(getBatch, function(err, data) {
	if (err) {
		console.log(err);
		process.exit(ERROR);
	} else {
		if (data && data.Item) {
			console.log(JSON.stringify(data.Item));
		} else {
			console.log("No Batch " + thisBatchId + " found in " + setRegion);
		}
	}

	process.exit(OK);
});
