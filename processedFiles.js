#!/usr/bin/env node

/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var aws = require('aws-sdk');
require('./constants');
var args = require('minimist')(process.argv.slice(2));
var common = require('./common');

var setRegion = args.region;
var queryOption = args.query;
var deleteOption = args.delete;
var reproOption = args.reprocess;
var file = args.file

if (!setRegion || (!queryOption && !deleteOption && !reproOption) || !file) {
    console.log("You must provide an AWS Region Code (--region), Query (--query), Delete (--delete), or Reprocess (--reprocess) option, and the specified Filename (--file)");
    process.exit(-1);
}

var dynamoDB = new aws.DynamoDB({
    apiVersion: '2012-08-10',
    region: setRegion
});

var s3 = new aws.S3({
    apiVersion: '2006-03-01',
    region: setRegion
});

var doExit = function (err, data, message) {
    if (err) {
        console.log(err);
        process.exit(error);
    } else {
        if (data && data.Item) {
            console.log(JSON.stringify(data.Item));
        }
        if (message) {
            console.log(message);
        }
    }
};

if (deleteOption) {
    common.deleteFile(dynamoDB, setRegion, file, function (err) {
        doExit(err)
    });
} else if (reproOption) {
    common.reprocessFile(dynamoDB, s3, setRegion, file, function (err) {
        doExit(err)
    });
} else if (queryOption) {
    common.queryFile(dynamoDB, setRegion, file, function (err, data) {
        doExit(err, data)
    });
} 