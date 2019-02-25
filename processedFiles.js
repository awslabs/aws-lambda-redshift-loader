#!/usr/bin/env node

/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */
let fileProcessingUtils = require('./fileProcessingUtils');

/** function to wrap up how we want to exit the module from the command line */
function doExit(err, data, message) {
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
}

/* process arguments provided on the command line */
let args = require('minimist')(process.argv.slice(2));
let setRegion = args.region;
let queryOption = args.query;
let deleteOption = args.delete;
let reproOption = args.reprocess;
let reproPrefix = args.reprocessPrefix;
let file = args.file;
let bucket = args.bucket;
let prefix = args.prefix;
let regex = args.regex;

if (!setRegion || (!queryOption && !deleteOption && !(reproOption  || reproPrefix)) || !(file || (bucket && prefix))) {
    console.log("You must provide an AWS Region Code (--region), Query (--query), Delete (--delete), Reprocess a File (--reprocess), or Reprocess an entire Prefix (--reprocessPrefix) option, and the specified Filename (--file) or Bucket and Prefix (--bucket --prefix). When reprocessing a prefix, you can also include a Regular Expression Filter (--regex).");
    process.exit(-1);
}

if (deleteOption) {
    fileProcessingUtils.deleteFile(setRegion, file, doExit.bind(undefined));
} else if (reproOption) {
    fileProcessingUtils.reprocessFile(setRegion, file, doExit.bind(undefined));
} else if (queryOption) {
    fileProcessingUtils.queryFile(setRegion, file, doExit.bind(undefined));
} else if (reproPrefix) {
    fileProcessingUtils.reprocessS3Prefix(setRegion, bucket, prefix, regex, doExit.bind(undefined));
}