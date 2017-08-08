/*
    Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

/**
 * Use config file and setup the DynamoDB table entry for the configuration
 */
var pjson = require('./package.json');
var aws = require('aws-sdk');
require('./constants');
var common = require('./common');
var async = require('async');
var uuid = require('uuid');
var dynamoDB;
var s3;
var lambda;
var kmsCrypto = require('./kmsCrypto');
var setRegion;

var configJson  = process.argv[2] || './config.json';
var setupConfig = require(configJson);

dynamoConfig = {
    TableName : configTable,
    Item : {
	currentBatch : {
	    S : uuid.v4()
	},
	version : {
	    S : pjson.version
	},
	loadClusters : {
	    L : [ {
		M : {}
	    } ]
	}
    }
};

// fake rl for common.js dependency
var rl = {
    close : function() {
	// fake close function
    }
};

var qs = [];

q_region = function(callback) {
    var regionsArray = [ "ap-northeast-1", "ap-southeast-1", "ap-southeast-2", "eu-central-1", "eu-west-1", "sa-east-1", "us-east-1", "us-west-1", "us-west-2" ];
    // region for the configuration
    if (common.blank(setupConfig.region) !== null) {
	common.validateArrayContains(regionsArray, setupConfig.region.toLowerCase(), rl);

	setRegion = setupConfig.region.toLowerCase();

	// configure dynamo db and kms for the correct region
	dynamoDB = new aws.DynamoDB({
	    apiVersion : '2012-08-10',
	    region : setRegion
	});
	kmsCrypto.setRegion(setRegion);
	s3 = new aws.S3({
	apiVersion : '2006-03-01'
	});
	lambda = new aws.Lambda({
	apiVersion : '2015-03-31',
	region : setRegion
	});
	callback(null);
    } else {
	console.log('You must provide a region from ' + regionsArray.toString())
    }
};

q_s3Prefix = function(callback) {
    // the S3 Bucket & Prefix to watch for files
    common.validateNotNull(setupConfig.s3Prefix, 'You Must Provide an S3 Bucket Name, and optionally a Prefix', rl);

    // setup prefix to be * if one was not provided
    var stripped = setupConfig.s3Prefix.replace(new RegExp('s3://', 'g'), '');
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

    callback(null);
};

q_filenameFilter = function(callback) {
    // a Filename Filter Regex
    if (common.blank(setupConfig.filenameFilter) !== null) {
	dynamoConfig.Item.filenameFilterRegex = {
	    S : setupConfig.filenameFilter
	};
    }
    callback(null);
};

q_clusterEndpoint = function(callback) {
    // the Cluster Endpoint
    common.validateNotNull(setupConfig.clusterEndpoint, 'You Must Provide a Cluster Endpoint', rl);
    dynamoConfig.Item.loadClusters.L[0].M.clusterEndpoint = {
	S : setupConfig.clusterEndpoint
    };
    callback(null);
};

q_clusterPort = function(callback) {
    // the Cluster Port
    dynamoConfig.Item.loadClusters.L[0].M.clusterPort = {
	N : '' + common.getIntValue(setupConfig.clusterPort, rl)
    };
    callback(null);
};

q_clusterUseSSL = function(callback) {
    // Does your cluster use SSL (Y/N)
    dynamoConfig.Item.loadClusters.L[0].M.useSSL = {
	BOOL : common.getBooleanValue(setupConfig.clusterUseSSL)
    };
    callback(null);
};

q_clusterDB = function(callback) {
    // the Database Name
    if (common.blank(setupConfig.clusterDB) !== null) {
	dynamoConfig.Item.loadClusters.L[0].M.clusterDB = {
	    S : setupConfig.clusterDB
	};
    }
    callback(null);
};

q_userName = function(callback) {
    // the Database Username
    common.validateNotNull(setupConfig.userName, 'You Must Provide a Username', rl);
    dynamoConfig.Item.loadClusters.L[0].M.connectUser = {
	S : setupConfig.userName
    };
    callback(null);
};

q_userPwd = function(callback) {
    // the Database Password
    common.validateNotNull(setupConfig.userPwd, 'You Must Provide a Password', rl);

    kmsCrypto.encrypt(setupConfig.userPwd, function(err, ciphertext) {
	if (err) {
	    console.log(JSON.stringify(err));
	    process.exit(ERROR);
	} else {
	    dynamoConfig.Item.loadClusters.L[0].M.connectPassword = {
		S : kmsCrypto.toLambdaStringFormat(ciphertext)
	    };
	    callback(null);
	}
    });
};

q_table = function(callback) {
    // the Table to be Loaded
    common.validateNotNull(setupConfig.table, 'You Must Provide a Table Name', rl);
    dynamoConfig.Item.loadClusters.L[0].M.targetTable = {
	S : setupConfig.table
    };
    callback(null);
};

q_columnList = function(callback) {
    // the comma-delimited column list (optional)
    if (setupConfig.columnList && common.blank(setupConfig.columnList) !== null) {
	dynamoConfig.Item.loadClusters.L[0].M.columnList = {
	    S : setupConfig.columnList
	};
	callback(null);
    } else {
	callback(null);
    }
};

q_truncateTable = function(callback) {
    // Should the Table be Truncated before Load? (Y/N)
    dynamoConfig.Item.loadClusters.L[0].M.truncateTarget = {
	BOOL : common.getBooleanValue(setupConfig.truncateTable)
    };
    callback(null);
};

q_df = function(callback) {
    // the Data Format (CSV, JSON or AVRO)
    common.validateArrayContains([ 'CSV', 'JSON', 'AVRO' ], setupConfig.df.toUpperCase(), rl);
    dynamoConfig.Item.dataFormat = {
	S : setupConfig.df.toUpperCase()
    };
    callback(null);
};

q_csvDelimiter = function(callback) {
    if (dynamoConfig.Item.dataFormat.S === 'CSV') {
	// the CSV Delimiter
	common.validateNotNull(setupConfig.csvDelimiter, 'You Must the Delimiter for CSV Input', rl);
	dynamoConfig.Item.csvDelimiter = {
	    S : setupConfig.csvDelimiter
	};
	callback(null);
    } else {
	callback(null);
    }
};

q_jsonPaths = function(callback) {
    if (dynamoConfig.Item.dataFormat.S === 'JSON' || dynamoConfig.Item.dataFormat.S === 'AVRO') {
	// the JSON Paths File Location on S3 (or NULL for Auto)
	if (common.blank(setupConfig.jsonPaths) !== null) {
	    dynamoConfig.Item.jsonPath = {
		S : setupConfig.jsonPaths
	    };
	}
	callback(null);
    } else {
	callback(null);
    }
};

q_manifestBucket = function(callback) {
    // the S3 Bucket for Redshift COPY Manifests
    common.validateNotNull(setupConfig.manifestBucket, 'You Must Provide a Bucket Name for Manifest File Storage', rl);
    dynamoConfig.Item.manifestBucket = {
	S : setupConfig.manifestBucket
    };
    callback(null);
};

q_manifestPrefix = function(callback) {
    // the Prefix for Redshift COPY Manifests
    common.validateNotNull(setupConfig.manifestPrefix, 'You Must Provide a Prefix for Manifests', rl);
    dynamoConfig.Item.manifestKey = {
	S : setupConfig.manifestPrefix
    };
    callback(null);
};

q_failedManifestPrefix = function(callback) {
    // the Prefix to use for Failed Load Manifest Storage
    common.validateNotNull(setupConfig.failedManifestPrefix, 'You Must Provide a Prefix for Manifests', rl);
    dynamoConfig.Item.failedManifestKey = {
	S : setupConfig.failedManifestPrefix
    };
    callback(null);
};

q_accessKey = function(callback) {
    // the Access Key used by Redshift to get data from S3.
    // If NULL then Lambda execution role credentials will be used
    if (!setupConfig.accessKey) {
	callback(null);
    } else {
	dynamoConfig.Item.accessKeyForS3 = {
	    S : setupConfig.accessKey
	};
	callback(null);
    }
};

q_secretKey = function(callback) {
    // the Secret Key used by Redshift to get data from S3.
    // If NULL then Lambda execution role credentials will be used
    if (!setupConfig.secretKey) {
	callback(null);
    } else {
	kmsCrypto.encrypt(setupConfig.secretKey, function(err, ciphertext) {
	    if (err) {
		console.log(JSON.stringify(err));
		process.exit(ERROR);
	    } else {
		dynamoConfig.Item.secretKeyForS3 = {
		    S : kmsCrypto.toLambdaStringFormat(ciphertext)
		};
		callback(null);
	    }
	});
    }
};

q_symmetricKey = function(callback) {
    // If Encrypted Files are used, Enter the Symmetric Master Key Value
    if (setupConfig.symmetricKey && common.blank(setupConfig.symmetricKey) !== null) {
	kmsCrypto.encrypt(setupConfig.symmetricKey, function(err, ciphertext) {
	    if (err) {
		console.log(JSON.stringify(err));
		process.exit(ERROR);
	    } else {
		dynamoConfig.Item.masterSymmetricKey = {
		    S : kmsCrypto.toLambdaStringFormat(ciphertext)
		};
		callback(null);
	    }
	});
    } else {
	callback(null);
    }
};

q_failureTopic = function(callback) {
    // the SNS Topic ARN for Failed Loads
    if (common.blank(setupConfig.failureTopic) !== null) {
	dynamoConfig.Item.failureTopicARN = {
	    S : setupConfig.failureTopic
	};
    }
    callback(null);
};

q_successTopic = function(callback) {
    // the SNS Topic ARN for Successful Loads
    if (common.blank(setupConfig.successTopic) !== null) {
	dynamoConfig.Item.successTopicARN = {
	    S : setupConfig.successTopic
	};
    }
    callback(null);
};

q_batchSize = function(callback) {
    // How many files should be buffered before loading?
    if (common.blank(setupConfig.batchSize) !== null) {
	dynamoConfig.Item.batchSize = {
	    N : '' + common.getIntValue(setupConfig.batchSize, rl)
	};
    }
    callback(null);
};

q_batchBytes = function(callback) {
    // Batches can be buffered up to a specified size. How large should a batch
    // be before processing (bytes)?
    if (common.blank(setupConfig.batchSizeBytes) !== null) {
	dynamoConfig.Item.batchSizeBytes = {
	    N : '' + common.getIntValue(setupConfig.batchSizeBytes, rl)
	};
    }
    callback(null);
};

q_batchTimeoutSecs = function(callback) {
    // How old should we allow a Batch to be before loading (seconds)?
    if (common.blank(setupConfig.batchTimeoutSecs) !== null) {
	dynamoConfig.Item.batchTimeoutSecs = {
	    N : '' + common.getIntValue(setupConfig.batchTimeoutSecs, rl)
	};
    }
    callback(null);
};

q_copyOptions = function(callback) {
    // Additional Copy Options to be added
    if (common.blank(setupConfig.copyOptions) !== null) {
	dynamoConfig.Item.copyOptions = {
	    S : setupConfig.copyOptions
	};
    }
    callback(null);
};

last = function(callback) {
    rl.close();

    setup(null, callback);
};

setup = function(overrideConfig, callback) {
    // set which configuration to use
    var useConfig = undefined;
    if (overrideConfig) {
	useConfig = overrideConfig;
    } else {
	useConfig = dynamoConfig;
    }
	common.setup(useConfig, dynamoDB, s3, lambda, callback);
};
// export the setup module so that customers can programmatically add new
// configurations
exports.setup = setup;

qs.push(q_region);
qs.push(q_s3Prefix);
qs.push(q_filenameFilter);
qs.push(q_clusterEndpoint);
qs.push(q_clusterPort);
qs.push(q_clusterUseSSL);
qs.push(q_clusterDB);
qs.push(q_table);
qs.push(q_columnList);
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
qs.push(q_batchBytes);
qs.push(q_batchTimeoutSecs);
qs.push(q_copyOptions);
qs.push(q_symmetricKey);

// always have to have the 'last' function added to halt the readline channel
// and run the setup
qs.push(last);

// call the first function in the function list, to invoke the callback
// reference chain
async.waterfall(qs);
