#!/usr/bin/env node
/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var aws = require('aws-sdk');
var async = require('async');
require('./constants');
var common = require('./common');

if (process.argv.length < 4) {
    console.log("You must provide an AWS Region Code, Batch ID, and configured Input Location");
    process.exit(ERROR);
}
var setRegion = process.argv[2];
var thisBatchId = process.argv[3];
var prefix = process.argv[4];

// connect to dynamo db and s3
var dynamoDB = new aws.DynamoDB({
    apiVersion: '2012-08-10',
    region: setRegion
});
var s3 = new aws.S3({
    apiVersion: '2006-03-01',
    region: setRegion
});

var updateBatchStatus = function (thisBatchId, err, results) {
    if (err) {
        console.log(JSON.stringify(err));
        process.exit(ERROR);
    } else {
        var updateBatchStatus = {
            Key: {
                batchId: {
                    S: thisBatchId,
                },
                s3Prefix: {
                    S: prefix
                }
            },
            TableName: batchTable,
            AttributeUpdates: {
                status: {
                    Action: 'PUT',
                    Value: {
                        S: 'reprocessed'
                    }
                },
                lastUpdate: {
                    Action: 'PUT',
                    Value: {
                        N: '' + common.now()
                    }
                }
            },
            // the batch to be unlocked must be in locked or error state - we
            // can't reopen 'complete' batches
            Expected: {
                status: {
                    AttributeValueList: [{
                        S: 'locked'
                    }, {
                        S: 'error'
                    }],
                    ComparisonOperator: 'IN'
                }
            }
        };

        dynamoDB.updateItem(updateBatchStatus, function (err, data) {
            if (err) {
                console.log(JSON.stringify(err));
                process.exit(ERROR);
            } else {
                console.log("Batch " + thisBatchId + " Submitted for Reprocessing");
                process.exit(OK);
            }
        })
    }
};

// fetch the batch
var getBatch = {
    Key: {
        batchId: {
            S: thisBatchId,
        },
        s3Prefix: {
            S: prefix
        }
    },
    TableName: batchTable,
    ConsistentRead: true
};

dynamoDB.getItem(getBatch, function (err, data) {
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
                var batchEntries = data.Item.entries.SS;

                if (!data.Item.entries.SS) {
                    console.log("Batch is Empty!");
                } else {
                    async.map(data.Item.entries.SS, common.inPlaceCopyFile.bind(undefined, s3, thisBatchId), updateBatchStatus.bind(undefined, thisBatchId));
                }
            }
        } else {
            console.log("Unable to retrieve batch " + thisBatchId + " for prefix " + prefix);
            process.exit(ERROR);
        }
    }
});
