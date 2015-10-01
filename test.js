/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var lambda = require('./index');

event = {
	"Records" : [ {
		"eventVersion" : "2.0",
		"eventSource" : "aws:s3",
		"awsRegion" : "eu-west-1",
		"eventTime" : "1970-01-01T00:00:00.000Z",
		"eventName" : "ObjectCreated:Put",
		"userIdentity" : {
			"principalId" : "AIDAJDPLRKLG7UEXAMPLE"
		},
		"requestParameters" : {
			"sourceIPAddress" : "127.0.0.1"
		},
		"responseElements" : {
			"x-amz-request-id" : "C3D13FE58DE4C810",
			"x-amz-id-2" : "FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"
		},
		"s3" : {
			"s3SchemaVersion" : "1.0",
			"configurationId" : "testConfigRule",
			"bucket" : {
				"name" : "lambda-redshift-loader-test",
				"ownerIdentity" : {
					"principalId" : "A3NL1KOZZKExample"
				},
				"arn" : "arn:aws:s3:::mybucket"
			},
			"object" : {
				"key" : "input/sample-redshift-file+for-lambda-loader38.csv",
				"size" : 1024,
				"eTag" : "d41d8cd98f00b204e9800998ecf8427e"
			}
		}
	} ]
};

function context() {}
context.done = function(status, message) {
	console.log("Context Closure Message: " + message);

	if (status && status !== null) {
		process.exit(-1);
	} else {
		process.exit(0);
	}
};

context.getRemainingTimeInMillis = function() {
	return 60000;
}

lambda.handler(event, context);