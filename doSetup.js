/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */
var aws = require('aws-sdk');
var common = require('./common');
var pjson = require('./package.json');
var uuid = require('node-uuid');

// configure dynamo db, kms, s3 and lambda for the correct region
var setRegion = "eu-west-1";
var dynamoDB = new aws.DynamoDB({
    apiVersion : '2012-08-10',
    region : setRegion
});
var s3 = new aws.S3({
    apiVersion : '2006-03-01'
});
var lambda = new aws.Lambda({
    apiVersion : '2015-03-31',
    region : setRegion
});
var dynamoConfig = {
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
		M : {

		}
	    } ]
	}
    }
};

dynamoConfig.Item.s3Prefix = {
    S : "meyersi-ire/AMS"
};
dynamoConfig.Item.loadClusters.L[0].M.clusterEndpoint = {
    S : "localhost"
};
dynamoConfig.Item.loadClusters.L[0].M.clusterPort = {
    N : '' + 5439
};
dynamoConfig.Item.loadClusters.L[0].M.useSSL = {
    BOOL : false
};
dynamoConfig.Item.loadClusters.L[0].M.clusterDB = {
    S : "master"
};
dynamoConfig.Item.loadClusters.L[0].M.connectUser = {
    S : "master"
};
dynamoConfig.Item.loadClusters.L[0].M.connectPassword = {
    S : "unencryptedUnusuablePassword"
};
dynamoConfig.Item.loadClusters.L[0].M.targetTable = {
    S : "ian_test"
};
dynamoConfig.Item.loadClusters.L[0].M.truncateTarget = {
    BOOL : false
};
dynamoConfig.Item.dataFormat = {
    S : "CSV"
};
dynamoConfig.Item.csvDelimiter = {
    S : ","
};
dynamoConfig.Item.manifestBucket = {
    S : "meyersi-ire"
};
dynamoConfig.Item.manifestKey = {
    S : "redshift/manifest"
};
dynamoConfig.Item.failedManifestKey = {
    S : "redshift/manifest/failed"
};
dynamoConfig.Item.batchSize = {
    N : '' + 1
};
dynamoConfig.Item.batchTimeoutSecs = {
    N : '' + 100
};

common.setup(dynamoConfig, dynamoDB, s3, lambda, function(err) {
    if (err) {
	console.log("Test Error");
	console.log(err);
    } else {
	console.log("Test OK");
    }
});