/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
var pjson = require('./package.json');
var common = require('./common');
var async = require('async');
var dynamoClient;
var debug = false;

exports.v1_v2 = function(err, s3Info, configPre, forwardCallback) {
	if (configPre.clusterEndpoint) {
		console.log("Upgrading to Version 2.x multi-cluster configuration");

		// bind the current config items into the new config map
		var clusterConfig = {
			M : {}
		};
		clusterConfig.M.clusterEndpoint = configPre.clusterEndpoint;
		clusterConfig.M.clusterPort = configPre.clusterPort;
		clusterConfig.M.clusterDB = configPre.clusterDB;
		clusterConfig.M.connectUser = configPre.connectUser;
		clusterConfig.M.connectPassword = configPre.connectPassword;
		clusterConfig.M.targetTable = configPre.targetTable;
		clusterConfig.M.truncateTarget = configPre.truncateTarget;

		// update dynamo, adding the new config map as 'loadClusters' and
		// removing
		// the old values
		var updateRequest = {
			Key : {
				s3Prefix : {
					S : s3Info.prefix
				}
			},
			TableName : configTable,
			UpdateExpression : "SET #loadCluster = :newLoadCluster, lastUpdate = :updateTime, #ver = :version "
					+ "REMOVE clusterEndpoint, clusterPort, clusterDB, connectUser, connectPassword, targetTable, truncateTarget",
			ExpressionAttributeValues : {
				":newLoadCluster" : {
					L : [ clusterConfig ]
				},
				":updateTime" : {
					S : common.getFormattedDate()
				},
				":version" : {
					S : pjson.version
				}
			},
			ExpressionAttributeNames : {
				"#ver" : 'version',
				"#loadCluster" : 'loadClusters'
			},
			/*
			 * current can't be the target version, or someone else has done an
			 * upgrade
			 */
			ConditionExpression : "(attribute_not_exists(#ver) or #ver != :version) and attribute_not_exists(#loadCluster)",
			// add the ALL_NEW return values so we have the
			// get the config after update
			ReturnValues : "ALL_NEW"
		};

		if (debug) {
			console.log(JSON.stringify(updateRequest));
		}

		dynamoClient.updateItem(updateRequest, function(err, data) {
			if (err) {
				if (err.code === conditionCheckFailed) {
					// no problem - configuration was upgraded by someone else
					// while
					// we were
					// running - requery and return
					var dynamoLookup = {
						Key : {
							s3Prefix : {
								S : s3Info.prefix
							}
						},
						TableName : configTable,
						ConsistentRead : true
					};

					dynamoClient.getItem(dynamoLookup, function(err, data) {
						forwardCallback(null, s3Info, data.Item);
					});
				} else {
					// unknown error - return the original configuration with
					// the
					// error
					forwardCallback(err, s3Info, configPre);
				}
			} else {
				// update was OK - go ahead with the new item returned by the
				// upgrade call
				forwardCallback(null, s3Info, data.Attributes);
			}
		});
	} else {
		// no upgrade required
		forwardCallback(null, s3Info, configPre);
	}
};

exports.fixEncryptedItemEncoding = function(err, s3Info, configPre, forwardCallback) {
	var willDoUpgrade = false;

	// convert the encrypted items from the previous native storage format to
	// base64 encoding
	loadClusters = [];

	var newMasterSymmetricKey;
	var newSecretKeyForS3;
	var updateExpressions = [];
	var expressionAttributeValues = {
		":updateTime" : {
			S : common.getFormattedDate()
		},
		":version" : {
			S : pjson.version
		}
	};

	if (configPre.masterSymmetricKey && configPre.masterSymmetricKey.S.indexOf('[') > -1) {
		willDoUpgrade = true;
		newMasterSymmetricKey = new Buffer(JSON.parse(configPre.masterSymmetricKey.S)).toString('base64');
		updateExpressions.push("masterSymmetricKey = :masterSymmetricKey");
		expressionAttributeValues[":masterSymmetricKey"] = {
			S : newMasterSymmetricKey
		};
	}
	if (configPre.secretKeyForS3 && configPre.secretKeyForS3.S.indexOf('[') > -1) {
		willDoUpgrade = true;
		newSecretKeyForS3 = new Buffer(JSON.parse(configPre.secretKeyForS3.S)).toString('base64');
		updateExpressions.push("secretKeyForS3 = :s3secretAccessKey");
		expressionAttributeValues[":s3secretAccessKey"] = {
			S : newSecretKeyForS3
		};
	}

	// upgrade each cluster entry's password for redshift
	if (configPre && configPre.loadClusters && configPre.loadClusters.L) {
		configPre.loadClusters.L.map(function (item) {
			if (item.M.connectPassword && item.M.connectPassword.S.indexOf('[') > -1) {
				willDoUpgrade = true;
				item.M.connectPassword.S = new Buffer(JSON.parse(item.M.connectPassword.S)).toString('base64');
			}

			loadClusters.push(item);
		});
	}

	if (loadClusters.length > 0) {
		expressionAttributeValues[":newLoadClusters"] = {
			L : loadClusters
		};
	}

	if (willDoUpgrade) {
		var updateRequest = {
			Key : {
				s3Prefix : {
					S : s3Info.prefix
				}
			},
			TableName : configTable,
			UpdateExpression : "SET #loadClusters = :newLoadClusters, lastUpdate = :updateTime, #ver = :version "
					+ (updateExpressions.length > 0 ? "," + updateExpressions.join(',') : ""),
			ExpressionAttributeValues : expressionAttributeValues,
			ExpressionAttributeNames : {
				"#ver" : 'version',
				"#loadClusters" : 'loadClusters'
			},
			/*
			 * current can't be the target version, or someone else has done an
			 * upgrade
			 */
			ConditionExpression : " #ver <> :version",
			// add the ALL_NEW return values so we have the
			// get the config after update
			ReturnValues : "ALL_NEW"
		};

		console.log("Upgrading to 2.4.x Base64 encoded encryption values");
		if (debug) {
			console.log(JSON.stringify(updateRequest));
		}

		dynamoClient.updateItem(updateRequest, function(err, data) {
			if (err) {
				if (err.code === conditionCheckFailed) {
					// no problem - configuration was upgraded by someone else
					// while
					// we were
					// running - requery and return
					var dynamoLookup = {
						Key : {
							s3Prefix : {
								S : s3Info.prefix
							}
						},
						TableName : configTable,
						ConsistentRead : true
					};

					dynamoClient.getItem(dynamoLookup, function(err, data) {
						forwardCallback(null, s3Info, data.Item);
					});
				} else {
					// unknown error - return the original configuration with
					// the error
					forwardCallback(err, s3Info, configPre);
				}
			} else {
				// update was OK - go ahead with the new item returned by the
				// upgrade call
				forwardCallback(null, s3Info, data.Attributes);
			}
		});
	} else {
		forwardCallback(null, s3Info, configPre)
	}
};

exports.upgradeAll = function(dynamoDB, s3Info, configPre, finalCallback) {
	dynamoClient = dynamoDB;

	// add required upgrades here
	var upgrades = [];
	/*
	 * upgrades.push(function(callback) { exports.v1_v2(null, s3Info, configPre,
	 * callback); });
	 */
	upgrades.push(function(callback) {
		exports.fixEncryptedItemEncoding(null, s3Info, configPre, callback);
	});

	// run all the upgrades in order
	async.waterfall(upgrades, function(err, s3Info, finalConfig) {
		if (err) {
			console.log(err);
		}

		// run the final callback
		finalCallback(err, s3Info, finalConfig);
	});
};