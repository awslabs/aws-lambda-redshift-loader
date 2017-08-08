/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */
require('dotenv').config();
const loaderInstanceName = process.env.LOADER_INSTANCE_NAME || '';
console.log("Loader instance name:", loaderInstanceName);
const loaderInstancePrefix = loaderInstanceName ? loaderInstanceName + "-" : "";

batchId = 'batchId';
currentBatch = 'currentBatch';
s3prefix = 's3Prefix';
lastUpdate = 'lastUpdate';
complete = 'complete';
locked = 'locked';
open = 'open';
error = 'error';
entries = 'entries';
status = 'status';
configTable = loaderInstancePrefix + 'LambdaRedshiftBatchLoadConfig';
batchTable = loaderInstancePrefix + 'LambdaRedshiftBatches';
batchStatusGSI = loaderInstancePrefix + 'LambdaRedshiftBatchStatus';
filesTable = loaderInstancePrefix + 'LambdaRedshiftProcessedFiles';
conditionCheckFailed = 'ConditionalCheckFailedException';
provisionedThroughputExceeded = 'ProvisionedThroughputExceededException';
deployedFunctionName = loaderInstancePrefix + 'LambdaRedshiftLoader';
INVALID_ARG = -1;
ERROR = -1;
OK = 0;
