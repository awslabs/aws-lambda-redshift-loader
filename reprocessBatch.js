/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var aws = require('aws-sdk');
require('./constants');

if (process.argv.length < 4) {
	console
			.log("You must provide an AWS Region Code, Batch ID, and configured Input Location");
	process.exit(ERROR);
}
var setRegion = process.argv[2];
var thisBatchId = process.argv[3];
var prefix = process.argv[4];

// connect to dynamo db and s3
var dynamoDB = new aws.DynamoDB({
	apiVersion : '2012-08-10',
	region : setRegion
});
var s3 = new aws.S3({
	apiVersion : '2006-03-01',
	region : setRegion
});

var batchEntries = undefined;

var processFile = function(index, thisBatchId) {
	// delete the processed file entry
	var fileItem = {
		Key : {
			loadFile : {
				S : batchEntries[index]
			}
		},
		TableName : filesTable
	};
	dynamoDB.deleteItem(fileItem, function(err, data) {
		if (err) {
			console.log(filesTable + " Delete Error");
			console.log(err);
			process.exit(ERROR);
		} else {
			// issue a same source/target copy command to S3, which will cause
			// Lambda to get a new event
			var bucketName = batchEntries[index].split("/")[0];
			var fileKey = batchEntries[index].replace(bucketName + "\/", "");
			var copySpec = {
				Metadata : {
					CopyReason : "AWS Lambda Redshift Loader Reprocess Batch "
							+ thisBatchId
				},
				MetadataDirective : "REPLACE",
				Bucket : bucketName,
				Key : fileKey,
				CopySource : batchEntries[index]
			};

			s3.copyObject(copySpec, function(err, data) {
				if (err) {
					console.log(err);
					process.exit(ERROR);
				} else {
					console.log("Submitted reprocess request for "
							+ batchEntries[index]);

					if (index + 1 < batchEntries.length) {
						// call this function with the next file entry index as
						// a reference
						processFile(index + 1);
					} else {
						console.log("Processed " + batchEntries.length
								+ " Files");
						process.exit(OK);
					}
				}
			});
		}
	});
};

// fetch the batch
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
			if (data.Item.status.S === open) {
				console.log("Cannot reprocess an Open Batch");
				process.exit(error);
			} else {
				// load the global batch entries so that we can process it in
				// callbacks
				batchEntries = data.Item.entries.SS;

				// call processFile with 0 index to tell it to process the first
				// item in
				// the array
				processFile(0, thisBatchId);
			}
		} else {
			console.log("Unable to retrieve batch " + thisBatchId
					+ " for prefix " + prefix);
			process.exit(ERROR);
		}
	}
});
