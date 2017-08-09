/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

/**
 * Use this script to reset the currentBatchId marker on the
 * LambdaRedshiftBatchLoadConfig table for a specific prefix. This will result
 * in the previous batch, if incomplete, to become 'disconnected' from the
 * automated processor and the files will NOT be loaded. If this is not 
 * 
 */
var async = require('async');
var aws = require('aws-sdk');
var common = require('./common');
var uuid = require('uuid');
var readline = require('readline');
require('./constants');
var rl = readline.createInterface({
    input : process.stdin,
    output : process.stdout
});

var usage = function() {
    console.log("You must provide an AWS Region Code, the configured Input Location, and the current Batch ID in order to reset.");
    process.exit(ERROR);
}

if (process.argv.length < 4) {
    usage();
}

var _setRegion = process.argv[2];
var _prefix = process.argv[3];
var _currentBatchId = process.argv[4];

if (!_currentBatchId || !_prefix) {
    usage();
}

var dynamoDB = new aws.DynamoDB({
    apiVersion : '2012-08-10',
    region : _setRegion
});

q_really_go = function(callback) {
    rl
	    .question(
		    'This function will reset the current batch marker for the specified prefix. Any batch entries which were linked to the previous batch will not be processed automatically, and you must re-inject them for processing using the reprocessBatch.js function. Press any key to continue, or ctrl-c NOW to exit > ',
		    function(answer) {
			callback(null);

		    });
};

last = function(callback) {
    rl.close();

    exports.resetBatchMarker(_prefix, _currentBatchId, callback);
};

qs = [];
qs.push(q_really_go);
qs.push(last);
async.waterfall(qs);

/* main exported interface - if you call this we assume you know what you are doing */
exports.resetBatchMarker = function(prefix, currentBatchId) {
    var getConfig = {
	Key : {
	    s3Prefix : {
		S : _prefix
	    }
	},
	TableName : configTable,
	ConsistentRead : true
    };

    dynamoDB.getItem(getConfig, function(err, data) {
	if (err) {
	    console.log(err);
	    process.exit(ERROR);
	} else {
	    if (!data || !data.Item || !data.Item.currentBatch) {
		console.log("Unable to find Configuration with S3 Prefix " + prefix + " in Region " + _setRegion);
	    } else {
		// update the current batch entry to a new marker value
		if (data.Item.currentBatch.S !== currentBatchId) {
		    console.log("Batch " + currentBatchId + " is not currently allocated as the open batch for Load Configuration on " + prefix
			    + ". Something has probably changed automatically, so we can't proceed.");
		    process.exit(ERROR);
		} else {
		    var newBatchId = uuid.v4();

		    var resetBatchParam = {
			Key : {
			    s3Prefix : {
				S : prefix
			    }
			},
			TableName : configTable,
			AttributeUpdates : {
			    currentBatch : {
				Action : 'PUT',
				Value : {
				    S : newBatchId
				}
			    },
			    lastUpdate : {
				Action : 'PUT',
				Value : {
				    N : '' + common.now()
				}
			    },
			    status : {
				Action : 'PUT',
				Value : {
				    S : open
				}
			    }
			}
		    };

		    dynamoDB.updateItem(resetBatchParam, function(err, data) {
			if (err) {
			    if (err.code === conditionCheckFailed) {
				console.log("Batch " + currentBatchId + " cannot be modified as the status is currently 'open' or 'complete' status");
			    } else {
				console.log(err);
				process.exit(ERROR);
			    }
			} else {
			    console.log("Batch " + currentBatchId + " rotated to value " + newBatchId + " and is ready for use");
			}

			process.exit(OK);
		    });
		}
	    }
	}
    });
}
