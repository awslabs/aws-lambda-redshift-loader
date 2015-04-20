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
var uuid = require('node-uuid');

dynamoConfig = {
	TableName : configTable,
	Item : {
		truncateTarget : {
			BOOL : false
		},
		currentBatch : {
			S : uuid.v4()
		}
	}
};

/* configuration of question prompts and config assignment */
var rl = readline.createInterface({
	input : process.stdin,
	output : process.stdout
});

qs = [];

q_region = function(i) {
	rl.question('Enter the Region for the Redshift Load Configuration > ', function(answer) {
		if (common.blank(answer) !== null) {
			common.validateArrayContains([ "ap-northeast-1", "ap-southeast-1", "ap-southeast-2", "eu-central-1", "eu-west-1",
					"sa-east-1", "us-east-1", "us-west-1", "us-west-2" ], answer.toLowerCase(), rl);

			setRegion = answer.toLowerCase();
		}
		qs[i + 1](i + 1);
	});
};

q_s3Prefix = function(i) {
	rl.question('Enter the S3 Bucket & Prefix to watch for files > ', function(answer) {
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

		dynamoConfig.Item.s3Prefix = {
			S : setPrefix
		};

		qs[i + 1](i + 1);
	});
};

q_filenameFilter = function(i) {
	rl.question('Enter a Filename Filter Regex > ', function(answer) {
		if (common.blank(answer) !== null) {
			dynamoConfig.Item.filenameFilterRegex = {
				S : answer
			};
		}
		qs[i + 1](i + 1);
	});
};

q_clusterEndpoint = function(i) {
	rl.question('Enter the Cluster Endpoint > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Cluster Endpoint', rl);
		dynamoConfig.Item.clusterEndpoint = {
			S : answer
		};
		qs[i + 1](i + 1);
	});
};

q_clusterPort = function(i) {
	rl.question('Enter the Cluster Port > ', function(answer) {
		dynamoConfig.Item.clusterPort = {
			N : '' + common.getIntValue(answer, rl)
		};
		qs[i + 1](i + 1);
	});
};

q_clusterDB = function(i) {
	rl.question('Enter the Database Name > ', function(answer) {
		if (common.blank(answer) !== null) {
			dynamoConfig.Item.clusterDB = {
				S : answer
			};
		}
		qs[i + 1](i + 1);
	});
};

q_userName = function(i) {
	rl.question('Enter the Database Username > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Username', rl);
		dynamoConfig.Item.connectUser = {
			S : answer
		};
		qs[i + 1](i + 1);
	});
};

q_userPwd = function(i) {
	rl.question('Enter the Database Password > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Password', rl);

		kmsCrypto.encrypt(answer, function(err, ciphertext) {
			dynamoConfig.Item.connectPassword = {
				S : kmsCrypto.toLambdaStringFormat(ciphertext)
			};
			qs[i + 1](i + 1);
		});
	});
};

q_table = function(i) {
	rl.question('Enter the Table to be Loaded > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Table Name', rl);
		dynamoConfig.Item.targetTable = {
			S : answer
		};
		qs[i + 1](i + 1);
	});
};

q_truncateTable = function(i) {
	rl.question('Should the Table be Truncated before Load? (Y/N) > ', function(answer) {
		dynamoConfig.Item.truncateTarget = {
			BOOL : common.getBooleanValue(answer)
		};
		qs[i + 1](i + 1);
	});
};

q_df = function(i) {
	rl.question('Enter the Data Format (CSV or JSON) > ', function(answer) {
		common.validateArrayContains([ 'CSV', 'JSON' ], answer.toUpperCase(), rl);
		dynamoConfig.Item.dataFormat = {
			S : answer.toUpperCase()
		};
		qs[i + 1](i + 1);
	});
};

q_csvDelimiter = function(i) {
	if (dynamoConfig.Item.dataFormat.S === 'CSV') {
		rl.question('Enter the CSV Delimiter > ', function(answer) {
			common.validateNotNull(answer, 'You Must the Delimiter for CSV Input', rl);
			dynamoConfig.Item.csvDelimiter = {
				S : answer
			};
			qs[i + 1](i + 1);
		});
	} else {
		qs[i + 1](i + 1);
	}
};

q_jsonPaths = function(i) {
	if (dynamoConfig.Item.dataFormat.S === 'JSON') {
		rl.question('Enter the JSON Paths File Location on S3 (or NULL for Auto) > ', function(answer) {
			if (common.blank(answer) !== null) {
				dynamoConfig.Item.jsonPath = {
					S : answer
				};
			}
			qs[i + 1](i + 1);
		});
	} else {
		qs[i + 1](i + 1);
	}
};

q_manifestBucket = function(i) {
	rl.question('Enter the S3 Bucket for Redshift COPY Manifests > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Bucket Name for Manifest File Storage', rl);
		dynamoConfig.Item.manifestBucket = {
			S : answer
		};
		qs[i + 1](i + 1);
	});
};

q_manifestPrefix = function(i) {
	rl.question('Enter the Prefix for Redshift COPY Manifests > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Prefix for Manifests', rl);
		dynamoConfig.Item.manifestKey = {
			S : answer
		};
		qs[i + 1](i + 1);
	});
};

q_failedManifestPrefix = function(i) {
	rl.question('Enter the Prefix to use for Failed Load Manifest Storage > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Prefix for Manifests', rl);
		dynamoConfig.Item.failedManifestKey = {
			S : answer
		};
		qs[i + 1](i + 1);
	});
};

q_accessKey = function(i) {
	rl.question('Enter the Access Key used by Redshift to get data from S3 > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide an Access Key', rl);
		dynamoConfig.Item.accessKeyForS3 = {
			S : answer
		};
		qs[i + 1](i + 1);
	});
};

q_secretKey = function(i) {
	rl.question('Enter the Secret Key used by Redshift to get data from S3 > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Secret Key', rl);

		kmsCrypto.encrypt(answer, function(err, ciphertext) {
			dynamoConfig.Item.secretKeyForS3 = {
				S : kmsCrypto.toLambdaStringFormat(ciphertext)
			};
			qs[i + 1](i + 1);
		});
	});
};

q_failureTopic = function(i) {
	rl.question('Enter the SNS Topic ARN for Failed Loads > ', function(answer) {
		if (common.blank(answer) !== null) {
			dynamoConfig.Item.failureTopicARN = {
				S : answer
			};
		}
		qs[i + 1](i + 1);
	});
};

q_successTopic = function(i) {
	rl.question('Enter the SNS Topic ARN for Successful Loads > ', function(answer) {
		if (common.blank(answer) !== null) {
			dynamoConfig.Item.successTopicARN = {
				S : answer
			};
		}
		qs[i + 1](i + 1);
	});
};

q_batchSize = function(i) {
	rl.question('How many files should be buffered before loading? > ', function(answer) {
		if (common.blank(answer) !== null) {
			dynamoConfig.Item.batchSize = {
				N : '' + common.getIntValue(answer, rl)
			};
		}
		qs[i + 1](i + 1);
	});
};

q_batchTimeoutSecs = function(i) {
	rl.question('How old should we allow a Batch to be before loading (seconds)? > ', function(answer) {
		if (common.blank(answer) !== null) {
			dynamoConfig.Item.batchTimeoutSecs = {
				N : '' + common.getIntValue(answer, rl)
			};
		}
		qs[i + 1](i + 1);
	});
};

q_copyOptions = function(i) {
	rl.question('Additional Copy Options to be added > ', function(answer) {
		if (common.blank(answer) !== null) {
			dynamoConfig.Item.copyOptions = {
				S : answer
			};
		}
		qs[i + 1](i + 1);
	});
};

last = function(i) {
	rl.close();

	setup();
};

setup = function(overrideConfig) {
	dynamoDB = new aws.DynamoDB({
		apiVersion : '2012-08-10',
		region : setRegion
	});

	// set which configuration to use
	var useConfig = undefined;
	if (overrideConfig) {
		useConfig = overrideConfig;
	} else {
		useConfig = dynamoConfig;
	}
	var configWriter = common.writeConfig(setRegion, dynamoDB, useConfig);
	common.createTables(dynamoDB, configWriter);
};
// export the setup module so that customers can programmatically add new
// configurations
exports.setup = setup;

qs.push(q_region);
qs.push(q_s3Prefix);
qs.push(q_filenameFilter);
qs.push(q_clusterEndpoint);
qs.push(q_clusterPort);
qs.push(q_clusterDB);
qs.push(q_table);
qs.push(q_truncateTable);
qs.push(q_userName);
qs.push(q_userPwd);
qs.push(q_df);
qs.push(q_csvDelimiter);
qs.push(q_jsonPaths);
qs.push(q_manifestBucket);
qs.push(q_manifestPrefix);
qs.push(q_failedManifestPrefix);
qs.push(q_accessKey);
qs.push(q_secretKey);
qs.push(q_successTopic);
qs.push(q_failureTopic);
qs.push(q_batchSize);
qs.push(q_batchTimeoutSecs);
qs.push(q_copyOptions);

// always have to have the 'last' function added to halt the readline channel
// and run the setup
qs.push(last);

// call the first function in the function list, to invoke the callback
// reference chain
qs[0](0);