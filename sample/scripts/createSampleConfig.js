/**
 * Ask questions of the end user via STDIN and then setup the dynamo DB table
 * entry for the configuration when done
 */
var readline = require('readline');
var aws = require('aws-sdk');
var dynamoDB;
require('../../constants');
var kmsCrypto = require('../../kmsCrypto');
var setRegion = 'us-east-1';
var common = require('../../common');
var async = require('async');
var uuid = require('node-uuid');
kmsCrypto.setRegion();

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
		batchTimeoutSecs : {
			N : "60"
		},
		connectUser : {
			S : "test_lambda_load_user"
		}
	}
};

kmsCrypto.encrypt("Change-me1!", function(err, ciphertext) {
	dynamoConfig.Item.connectPassword = {
		S : kmsCrypto.toLambdaStringFormat(ciphertext)
	};
});

/* configuration of question prompts and config assignment */
var rl = readline.createInterface({
	input : process.stdin,
	output : process.stdout
});

qs = [];

q_region = function(callback) {
	rl.question('Enter the Region for the Redshift Load Configuration > ', function(answer) {
		if (common.blank(answer) !== null) {
			setRegion = answer;
		} else {
			console.log('Using the default region ' + setRegion);
		}
		callback();
	});
};

q_s3Prefix = function(callback) {
	rl.question('Enter the S3 Bucket to use for the Sample Input > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide an S3 Bucket Name');

		dynamoConfig.Item.s3Prefix = {
			S : answer.replace(new RegExp('s3://', 'g'), '') + "/input"
		};

		// use the same bucket for manifest files
		dynamoConfig.Item.manifestBucket = {
			S : dynamoConfig.Item.s3Prefix.S.split("/")[0]
		};

		callback();
	});
};

q_clusterEndpoint = function(callback) {
	// use environment variable if we can
	if (process.env['CLUSTER_ENDPOINT'] === undefined || process.env['CLUSTER_ENDPOINT'] === null) {
		rl.question('Enter the Cluster Endpoint > ', function(answer) {
			common.validateNotNull(answer, 'You Must Provide a Cluster Endpoint');
			dynamoConfig.Item.clusterEndpoint = {
				S : answer
			};
			callback();
		});
	} else {
		dynamoConfig.Item.clusterEndpoint = {
			S : process.env['CLUSTER_ENDPOINT']
		};
		callback();
	}
};

q_clusterPort = function(callback) {
	if (process.env['CLUSTER_PORT'] === undefined) {
		rl.question('Enter the Cluster Port > ', function(answer) {
			dynamoConfig.Item.clusterPort = {
				N : '' + common.getIntValue(answer)
			};
			callback();
		});
	} else {
		dynamoConfig.Item.clusterPort = {
			N : process.env['CLUSTER_PORT']
		};
		callback();
	}
};

q_clusterDB = function(callback) {
	if (process.env['CLUSTER_DB'] === undefined) {
		rl.question('Enter the Database Name > ', function(answer) {
			if (common.blank(answer) !== null) {
				dynamoConfig.Item.clusterDB = {
					S : answer
				};
			}
			callback();
		});
	} else {
		dynamoConfig.Item.clusterDB = {
			S : process.env['CLUSTER_DB']
		};
		callback();
	}
};

last = function(callback) {
	rl.close();

	setup(callback);
};

setup = function(callback) {
	dynamoDB = new aws.DynamoDB({
		apiVersion : '2012-08-10',
		region : setRegion
	});
	var configWriter = common.writeConfig(setRegion, dynamoDB, dynamoConfig, callback);
	common.createTables(dynamoDB, configWriter);
};

qs.push(q_region);
qs.push(q_s3Prefix);
qs.push(q_clusterEndpoint);
qs.push(q_clusterPort);
qs.push(q_clusterDB);

// always have to have the 'last' function added to halt the readline channel
// and run the setup
qs.push(last);

// call the first function in the function list, to invoke the callback
// reference chain
async.waterfall(qs);