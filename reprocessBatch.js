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
var batchOperations = require('./batchOperations');

var usage = function() {
    console.log("You must provide an AWS Region Code, Batch ID, and configured Input Location");
    console.log("You may also provide a list of files to be omitted from the reprocessing task");
    process.exit(ERROR);
}
if (process.argv.length < 4) {
    usage();
}
if (process.argv.length > 5) {
    console.log("You have provided too many arguments to the function");
    usage();
}

var setRegion = process.argv[2];
var thisBatchId = process.argv[3];
var prefix = process.argv[4];
var omitFiles;
if (process.argv.length == 5) {
    omitFiles = process.argv[5].split(",")
}

var s3 = new aws.S3({
    apiVersion: '2006-03-01',
    region: setRegion
});

batchOperations.reprocessBatch(prefix, thisBatchId, setRegion, omitFiles, function (err) {
    if (err) {
        console.log(err);
        process.exit(ERROR);
    } else {
        process.exit(OK);
    }
});