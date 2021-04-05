/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */
var batchOperations = require("./batchOperations");

var args = require('minimist')(process.argv.slice(2));

var setRegion = args.region;
var s3Prefix = args.s3Prefix;

batchOperations.cleanBatches(setRegion, s3Prefix, function (err, data) {
    if (err) {
        console.log("Error: " + err);
        process.exit(-1);
    } else {
        console.log("OK: Deletion of " + data.batchCountDeleted + " Batches");
        console.log("Deleted Batch Information:");
        console.log(JSON.stringify(data));

    }
})
