/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var aws = require('aws-sdk');
require('./constants');
var args = require('minimist')(process.argv.slice(2));

var setRegion = args.region;
var queryOption = args.query;
var deleteOption = args['delete'];
var file = args.file

if (!setRegion || (!queryOption && !deleteOption) || !file) {
    console.log("You must provide an AWS Region Code (--region), Query (-query) or Delete (-delete) option, and the specified Filename (--file)");
    process.exit(-1);
}

var dynamoDB = new aws.DynamoDB({
    apiVersion : '2012-08-10',
    region : setRegion
});

var fileItem = {
    Key : {
	loadFile : {
	    S : file
	}
    },
    TableName : filesTable
};

if (deleteOption) {
    dynamoDB.deleteItem(fileItem, function(err, data) {
	if (err) {
	    console.log(err);
	    process.exit(error);
	} else {
	    console.log("File Entry " + file + " deleted successfully");
	}
    });
} else if (queryOption) {
    dynamoDB.getItem(fileItem, function(err, data) {
	if (err) {
	    console.log(err);
	    process.exit(error);
	} else {
	    console.log(JSON.stringify(data.Item));
	}
    });
}