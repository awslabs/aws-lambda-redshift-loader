let aws = require('aws-sdk');
let common = require('./common');
let async = require('async');
require('./constants');

/** create clients for s3 and dynamodb */
function connect(setRegion, callback) {
    let dynamoDB = new aws.DynamoDB({
        apiVersion: '2012-08-10',
        region: setRegion
    });

    let s3 = new aws.S3({
        apiVersion: '2006-03-01',
        region: setRegion
    });

    callback(dynamoDB, s3);
}

/** function to delete a file */
function deleteFile(setRegion, file, callback) {
    connect(setRegion, function (dynamoDB, s3) {
        common.deleteFile(dynamoDB, s3, setRegion, file, callback);
    });
}

exports.deleteFile = deleteFile;


/** function to reprocess a file */
function reprocessFile(setRegion, file, callback) {
    connect(setRegion, function (dynamoDB, s3) {
        common.reprocessFile(dynamoDB, s3, setRegion, file, callback);
    });
}

exports.reprocessFile = reprocessFile;

function reprocessS3Prefix(setRegion, bucket, prefix, regexFilter, callback) {
    let matcher;
    if (regexFilter) {
        matcher = new RegExp(regexFilter);
    }
    let filesProcessed = 0;

    connect(setRegion, function (dynamoDB, s3) {
        let processing = true;
        let params = {
            Bucket: bucket,
            Prefix: prefix
        };
        async.whilst(function (test_cb) {
            test_cb(null, processing);
        }, function (whilstCallback) {
            s3.listObjectsV2(params, function (err, data) {
                if (err) {
                    whilstCallback(err);
                } else {
                    // for each returned object, check the filter regex
                    async.map(data.Contents, function (item, mapCallback) {
                        if ((matcher && matcher.test(item.Key)) || !matcher) {
                            filesProcessed++;
                            console.log("Requesting reprocess of " + bucket + "/" + item.Key);
                            common.reprocessFile(dynamoDB, s3, setRegion, bucket + "/" + item.Key, mapCallback);
                        }
                    }, function (err) {
                        if (err) {
                            whilstCallback(err)
                        } else {
                            if (data.IsTruncated === true) {
                                params.ContinuationToken = data.NextContinuationToken;
                            } else {
                                // data wasn't truncated, so we have all the results
                                processing = false;
                            }

                            whilstCallback();
                        }
                    });
                }
            });
        }, function (err) {
            callback(err, filesProcessed);
        });
    });
}

exports.reprocessS3Prefix = reprocessS3Prefix;

/** function to query files from the system and understand their status */
function queryFile(setRegion, file, callback) {
    connect(setRegion, function (dynamoDB) {
        common.queryFile(dynamoDB, setRegion, file, callback);
    });
}

exports.queryFile = queryFile;