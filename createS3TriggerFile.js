var debug = process.env['DEBUG'] || false;

require('./constants');
var aws = require('aws-sdk');
/* jshint -W069 */// suppress warnings about dot notation
var setRegion = process.env['AWS_REGION'];
var s3 = new aws.S3({
	apiVersion : '2006-03-01',
	region : setRegion
});
var dynamoDB = new aws.DynamoDB({
	apiVersion : '2012-08-10',
	region : setRegion
});

var async = require('async');

exports.handler = function(event, context) {
	"use strict";

	var dynamoConfigLookup = {
		TableName : configTable,
	};

	// read each config entry to create trigger file for each location
	dynamoDB.scan(dynamoConfigLookup, function(err, data) {
		if (err) {
			context.fail(err);
		} else {
			if (!data.Items) {
				console.log("Looks like you don't have any configured Prefix entries!");
				context.succeed();
			} else {
				// create a trigger file entry for each prefix
				async.each(data.Items, function(configItem, callback) {
					// only PUT a trigger file if loader has a regex so it will
					// get ignored
					if (configItem.filenameFilterRegex) {
						console.log("Processing config entry : " + configItem.s3Prefix.S);

						var bucketName = configItem.s3Prefix.S.split("/")[0];
						var fileKey = configItem.s3Prefix.S.replace(bucketName + "\/", "");

						// create a trigger file on S3
						exports.createTriggerFile(bucketName, fileKey, callback);
					} else {
						callback();
					}
				}, function(err) {
					if (err) {
						context.fail(err);
					} else {
						context.succeed();
					}
				});
			}
		}
	});

	/** function which will create a trigger file in the specified path */
	exports.createTriggerFile = function(bucketName, fileKey, callback) {
		var prefix = fileKey + "/lambda-redshift-trigger-file.dummy";

		var createParams = {
			Bucket : bucketName,
			Key : prefix,
			Body : 'AWS Lambda Redshift Loader Event Trigger File'
		};
		s3.putObject(createParams, function(err, data) {
			if (err) {
				console.log("Error uploading data to " + bucketName + "/" + prefix, err);
				callback(err);
			} else {
				console.log("Created Dummy file", bucketName + "/" + prefix);
				callback();
			}
		});
	};
};