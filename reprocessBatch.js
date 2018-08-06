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
var batchOperations = require('./batchOperations');

var args = require('minimist')(process.argv.slice(2));

var setRegion = args.region;
var thisBatchId = args.batchId;
var prefix = args.prefix;

if (!setRegion || !thisBatchId || !prefix) {
    usage();
}

var usage = function () {
    console.log("You must provide an AWS Region Code (--region), Batch ID (--batchId), and configured Input Location (--prefix)");
    console.log("You may also provide a list of files to be omitted from the reprocessing task");
    process.exit(ERROR);
}
var omitFiles;
if (args.omitFiles) {
    omitFiles = args.omitFiles.split(",")
}
batchOperations.reprocessBatch(prefix, thisBatchId, setRegion, omitFiles, function (err) {
    if (err) {
        console.log(err);
        process.exit(ERROR);
    } else {
        process.exit(OK);
    }
});