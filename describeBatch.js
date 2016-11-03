/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */
var args = require('minimist')(process.argv.slice(2));
var batchOperations = require("./batchOperations");

var setRegion = args.region;
var thisBatchId = args.batchId;
var prefix = args.s3prefix;

if (!setRegion || !thisBatchId || !prefix) {
    console.log("You must provide an AWS Region Code, Batch ID, and configured Input Location");
    process.exit(-1);
}

batchOperations.getBatch(setRegion, prefix, thisBatchId, function(err, data) {
    if (err) {
	console.log("ERROR:" + err);
	process.exit(-1);
    } else {
	console.log(JSON.stringify(data));
    }
});