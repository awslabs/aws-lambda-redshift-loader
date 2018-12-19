let aws = require('aws-sdk');
require('./constants');
let common = require('./common');

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


/** function to query files from the system and understand their status */
function queryFile(setRegion, file, callback) {
    connect(setRegion, function (dynamoDB) {
        common.queryFile(dynamoDB, setRegion, file, callback);
    });
}

exports.queryFile = queryFile;