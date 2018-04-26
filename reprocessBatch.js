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

if (process.argv.length < 4) {
    console.log("You must provide an AWS Region Code, Batch ID, and configured Input Location");
    process.exit(ERROR);
}
var setRegion = process.argv[2];
var thisBatchId = process.argv[3];
var prefix = process.argv[4];

var s3 = new aws.S3({
    apiVersion: '2006-03-01',
    region: setRegion
});

batchOperations.reprocessBatch(prefix, thisBatchId, setRegion, function (err) {
    if (err) {
        console.log(err);
        process.exit(ERROR);
    } else {
        process.exit(OK);
    }
});