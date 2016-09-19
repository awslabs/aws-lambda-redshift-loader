#!/bin/bash

ver=`cat package.json | grep version | cut -d: -f2 | sed -e "s/\"//g" | sed -e "s/ //g" | sed -e "s/\,//g"`

rm dist/AWSLambdaRedshiftLoader-$ver.zip

zip -r AWSLambdaRedshiftLoader-$ver.zip index.js common.js createS3TriggerFile.js constants.js kmsCrypto.js upgrades.js *.txt package.json node_modules/async node_modules/node-uuid node_modules/pg

mv AWSLambdaRedshiftLoader-$ver.zip dist