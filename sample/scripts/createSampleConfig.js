/**
 * Ask questions of the end user via STDIN and then setup the dynamo DB table
 * entry for the configuration when done
 */
var readline = require('readline');
var aws = require('aws-sdk');
var dynamoDB;
require('../../constants');
var crypto = require('lollyrock');
var setRegion = 'us-east-1';
var common = require('../../common');
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
		},
		targetTable : {
			S : "lambda_redshift_sample"
		},
		dataFormat : {
			S : "CSV"
		},
		csvDelimiter : {
			S : "|"
		},
		manifestKey : {
			S : "lambda-redshift/load-manifest"
		},
		failedManifestKey : {
			S : "lambda-redshift/failed-manifest"
		},
		batchSize : {
			N : "2"
		},
		connectUser : {
			S : "test_lambda_load_user"
		},
		connectPassword : {
			S : crypto.encrypt("Change-me1!")
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
			setRegion = answer;
		} else {
			console.log('Using the default region ' + setRegion);
		}
		qs[i + 1](i + 1);
	});
};

q_s3Prefix = function(i) {
	rl.question('Enter the S3 Bucket to use for the Sample Input > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide an S3 Bucket Name');

		dynamoConfig.Item.s3Prefix = {
			S : answer.replace(new RegExp('s3://', 'g'), '') + "/input"
		};

		// use the same bucket for manifest files
		dynamoConfig.Item.manifestBucket = {
			S : dynamoConfig.Item.s3Prefix.S.split("/")[0]
		};

		qs[i + 1](i + 1);
	});
};

q_clusterEndpoint = function(i) {
	// use environment variable if we can
	if (process.env['CLUSTER_ENDPOINT'] === undefined || process.env['CLUSTER_ENDPOINT'] === null) {
		rl.question('Enter the Cluster Endpoint > ', function(answer) {
			common.validateNotNull(answer, 'You Must Provide a Cluster Endpoint');
			dynamoConfig.Item.clusterEndpoint = {
				S : answer
			};
			qs[i + 1](i + 1);
		});
	} else {
		dynamoConfig.Item.clusterEndpoint = {
			S : process.env['CLUSTER_ENDPOINT']
		};
		qs[i + 1](i + 1);
	}
};

q_clusterPort = function(i) {
	if (process.env['CLUSTER_PORT'] === undefined) {
		rl.question('Enter the Cluster Port > ', function(answer) {
			dynamoConfig.Item.clusterPort = {
				N : '' + common.getIntValue(answer)
			};
			qs[i + 1](i + 1);
		});
	} else {
		dynamoConfig.Item.clusterPort = {
			N : process.env['CLUSTER_PORT']
		};
		qs[i + 1](i + 1);
	}
};

q_clusterDB = function(i) {
	if (process.env['CLUSTER_DB'] === undefined) {
		rl.question('Enter the Database Name > ', function(answer) {
			if (common.blank(answer) !== null) {
				dynamoConfig.Item.clusterDB = {
					S : answer
				};
			}
			qs[i + 1](i + 1);
		});
	} else {
		dynamoConfig.Item.clusterDB = {
			S : process.env['CLUSTER_DB']
		};
		qs[i + 1](i + 1);
	}
};

q_accessKey = function(i) {
	rl.question('Enter the Access Key used by Redshift to get data from S3 > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide an Access Key');
		dynamoConfig.Item.accessKeyForS3 = {
			S : answer
		};
		qs[i + 1](i + 1);
	});
};

q_secretKey = function(i) {
	rl.question('Enter the Secret Key used by Redshift to get data from S3 > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Secret Key');
		dynamoConfig.Item.secretKeyForS3 = {
			S : crypto.encrypt(answer)
		};
		qs[i + 1](i + 1);
	});
};

last = function(i) {
	rl.close();

	setup();
};

setup = function() {
	dynamoDB = new aws.DynamoDB({
		apiVersion : '2012-08-10',
		region : setRegion
	});

	var configWriter = common.writeConfig(setRegion, dynamoDB, dynamoConfig);
	common.createTables(dynamoDB, configWriter);
};

qs.push(q_region);
qs.push(q_s3Prefix);
qs.push(q_clusterEndpoint);
qs.push(q_clusterPort);
qs.push(q_clusterDB);
qs.push(q_accessKey);
qs.push(q_secretKey);

// always have to have the 'last' function added to halt the readline channel
// and run the setup
qs.push(last);

// call the first function in the function list, to invoke the callback
// reference chain
qs[0](0);