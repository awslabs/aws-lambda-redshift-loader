#!/usr/bin/env node

var common = require('./common');
var args = require('minimist')(process.argv.slice(2));
var aws = require('aws-sdk');
aws.config.update({
    region: args.region
});

// configure dynamo db, kms, s3 and lambda for the correct region

dynamoDB = new aws.DynamoDB({
    apiVersion: '2012-08-10',
    region: args.region
});


common.updateConfig(args.s3Prefix, args.configAttribute, args.configValue, dynamoDB, function (err) {
    if (err) {
        console.log(err);
        process.exit(-1);
    } else {
        if (args.configValue) {
            console.log("Updated Attribute " + args.configAttribute + " = " + args.configValue + " OK");
        } else {
            console.log("Removed Attribute " + args.configAttribute);
        }
        process.exit(0);
    }
});

