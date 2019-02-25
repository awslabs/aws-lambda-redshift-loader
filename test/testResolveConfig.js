/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var lambda = require('./index');

function context() {}
context.done = function(status, message) {
	console.log("Context Closure Message: " + JSON.stringify(message));

	if (status && status !== null) {
		process.exit(-1);
	} else {
		process.exit(0);
	}
};

context.getRemainingTimeInMillis = function() {
	return 60000;
}

lambda.resolveConfig("energy-streaming-demo/data/csv/capture_date=2015-09-28/capture_time=15/test/part-00000".transformHiveStylePrefix(), function(err, config) {
	if (err) {
		context.done(err);
	} else {
		context.done(null, "Found Config " + config.Item.s3Prefix.S + " OK");
	}
}, function(err) {
	if (err) {
		context.done(err);
	} else {
		context.done("ERROR", "Unable to resolve Configuration Entry");
	}
});