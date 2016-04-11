/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

/**
 * Ask questions of the end user via STDIN and then setup the dynamo DB table
 * entry for the configuration when done
 */
var readline = require('readline');
var aws = require('aws-sdk');
var dynamoDB;
require('./constants');
var kmsCrypto = require('./kmsCrypto');
var setRegion = 'us-east-1';
var common = require('./common');
var async = require('async');

// simple frame for the updated cluster config
var clusterConfig = {
	M : {

	}
};

var updateRequest = {
	Key : {
		s3Prefix : undefined
	},
	TableName : configTable,
	UpdateExpression : "SET loadClusters = list_append(loadClusters, :newLoadCluster),lastUpdate = :updateTime",
	ExpressionAttributeValues : {
		":newLoadCluster" : null,
		":updateTime" : {
			N : '' + common.now()
		}
	}
};

/* configuration of question prompts and config assignment */
var rl = readline.createInterface({
	input : process.stdin,
	output : process.stdout
});

var qs = [];

q_region = function(callback) {
	rl.question('Enter the Region for the Configuration > ', function(answer) {
		if (common.blank(answer) !== null) {
			common.validateArrayContains([ "ap-northeast-1", "ap-southeast-1", "ap-southeast-2", "eu-central-1", "eu-west-1",
					"sa-east-1", "us-east-1", "us-west-1", "us-west-2" ], answer.toLowerCase(), rl);

			setRegion = answer.toLowerCase();

			// configure dynamo db and kms to use this region
			dynamoDB = new aws.DynamoDB({
				apiVersion : '2012-08-10',
				region : setRegion
			});
			kmsCrypto.setRegion(setRegion);
			callback(null);
		}
	});
};

q_s3Prefix = function(callback) {
	rl.question('Enter the Configuration S3 Bucket & Prefix > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide an S3 Bucket Name, and optionally a Prefix', rl);

		// setup prefix to be * if one was not provided
		var stripped = answer.replace(new RegExp('s3://', 'g'), '');
		var elements = stripped.split("/");
		var setPrefix = undefined;

		if (elements.length === 1) {
			// bucket only so use "bucket" alone
			setPrefix = elements[0];
		} else {
			// right trim "/"
			setPrefix = stripped.replace(/\/$/, '');
		}

		// set the s3 prefix in the update request object
		updateRequest.Key.s3Prefix = {
			S : setPrefix
		};

		callback(null);
	});
};

q_clusterEndpoint = function(callback) {
	rl.question('Enter the Cluster Endpoint > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Cluster Endpoint', rl);
		clusterConfig.M.clusterEndpoint = {
			S : answer
		};

		callback(null);
	});
};

q_clusterPort = function(callback) {
	rl.question('Enter the Cluster Port > ', function(answer) {
		clusterConfig.M.clusterPort = {
			N : '' + common.getIntValue(answer, rl)
		};

		callback(null);
	});
};

q_clusterUseSSL = function(callback) {
	rl.question('Does your cluster use SSL (Y/N)  > ', function(answer) {
        clusterConfig.M.useSSL = {
            BOOL : common.getBooleanValue(answer)
        };
		callback(null);
	});
};


q_clusterDB = function(callback) {
	rl.question('Enter the Database Name > ', function(answer) {
		if (common.blank(answer) !== null) {
			clusterConfig.M.clusterDB = {
				S : answer
			};

			callback(null);
		}
	});
};

q_userName = function(callback) {
	rl.question('Enter the Database Username > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Username', rl);
		clusterConfig.M.connectUser = {
			S : answer
		};

		callback(null);
	});
};

q_userPwd = function(callback) {
	rl.question('Enter the Database Password > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Password', rl);

		kmsCrypto.encrypt(answer, function(err, ciphertext) {
			clusterConfig.M.connectPassword = {
				S : kmsCrypto.toLambdaStringFormat(ciphertext)
			};

			callback(null);
		});
	});
};

q_table = function(callback) {
	rl.question('Enter the Table to be Loaded > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Table Name', rl);
		clusterConfig.M.targetTable = {
			S : answer
		};

		callback(null);
	});
};

q_truncateTable = function(callback) {
	rl.question('Should the Table be Truncated before Load? (Y/N) > ', function(answer) {
		clusterConfig.M.truncateTarget = {
			BOOL : common.getBooleanValue(answer)
		};

		callback(null);
	});
};

last = function(callback) {
	rl.close();

	addClusterToPrefix(callback);
};

addClusterToPrefix = function(callback, overrideConfig) {
	var useConfig = clusterConfig;

	if (overrideConfig) {
		useConfig = overrideConfig;
	}

	// add the Expression Attribute Value for the new config section to the
	// request as a list of 1 Map item
	updateRequest.ExpressionAttributeValues[":newLoadCluster"] = {
		"L" : [ useConfig ]
	};

	// update the configuration
	common.updateConfig(setRegion, dynamoDB, updateRequest, callback);
};
exports.addClusterToPrefix = addClusterToPrefix;

// setup the question list for async
qs.push(q_region);
qs.push(q_s3Prefix);
qs.push(q_clusterEndpoint);
qs.push(q_clusterPort);
qs.push(q_clusterUseSSL);
qs.push(q_clusterDB);
qs.push(q_table);
qs.push(q_truncateTable);
qs.push(q_userName);
qs.push(q_userPwd);

// always have to have the 'last' function added to halt the readline channel
// and run the setup
qs.push(last);

// call the first function in the function list, to invoke the callback
// reference chain
async.waterfall(qs);