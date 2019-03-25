#!/bin/bash

ver=`cat package.json | grep version | cut -d: -f2 | sed -e "s/\"//g" | sed -e "s/ //g" | sed -e "s/\,//g"`

rm dist/AWSLambdaRedshiftLoader-$ver.zip

npm install --upgrade

zip -r AWSLambdaRedshiftLoader-$ver.zip failedBatchReprocessingLambda.js index.js batchOperations.js common.js createS3TriggerFile.js constants.js kmsCrypto.js upgrades.js *.txt package.json node_modules -x node_modules/aws-sdk/**\*

mv AWSLambdaRedshiftLoader-$ver.zip dist

echo "Build completed to dist/AWSLambdaRedshiftLoader-$ver.zip"
