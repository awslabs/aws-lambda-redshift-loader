console.log('Loading function');

var aws = require('aws-sdk');
var region = "us-west-2";
var s3 = new aws.S3({
	apiVersion : '2006-03-01',
	region : region
});

var dynamoDB = new aws.DynamoDB();

exports.handler = function(event, context) {

    // self contained lambda function that can be edited inline, hence hard code table here
	var dynamoConfigLookup = {
        TableName: "LambdaRedshiftBatchLoadConfig",
    };

    // read each config entry to create dummy file for each location
    dynamoDB.scan(dynamoConfigLookup, function(err, data) {
        data.Items.forEach(function(configItem) {
            // only PUT/COPY a dummy file if loader has a regex so it will get ignored
            if (typeof configItem.filenameFilterRegex != "undefined") {
                console.log("Processing config entry : " + configItem.s3Prefix.S);

                var bucketName = configItem.s3Prefix.S.split("/")[0];
                var fileKey = configItem.s3Prefix.S.replace(bucketName + "\/", "");
                var dummyFile = fileKey + "/_dummy";
                var getDummyFile = {
                    Bucket: bucketName,
                    Key: dummyFile
                };

                s3.headObject(getDummyFile, function (err, metadata) {
                    if (err && err.code === 'NotFound') {
                        createDummyFile(bucketName, dummyFile);
                    } else {
                        copyDummyFile(bucketName, dummyFile);
                    }
                });
            }
        });
    });

    function createDummyFile(bucketName, dummyFile) {
        console.log("Dummy file not found. Attempting to create one.");
        var createParams = {Bucket: bucketName, Key: dummyFile, Body: 'DummyFileContents'};
        s3.putObject(createParams, function(err, data) {
            if (err) {
                console.log("Error uploading data ", err);
                context.fail(err);
            }
            console.log("Created Dummy file", bucketName + "/" + dummyFile);
            context.succeed();
        });
    }

    function copyDummyFile(bucketName, dummyFile) {
        var copySpec = {
            Metadata : {
                CopyReason : "AWS Lambda Redshift Loader Dummy File Generator"
            },
            MetadataDirective : "REPLACE",
            Bucket : bucketName,
            Key : dummyFile,
            CopySource : bucketName + "/" + dummyFile
        };

        s3.copyObject(copySpec, function(err, data) {
            if (err) {
                console.log(err);
                context.fail(err);
            } else {
                console.log("Copied Dummy file", bucketName + "/" + dummyFile);
                context.succeed();
            }
        });
    }
};