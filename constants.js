/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */
 var lambdaPrefix = process.env.LAMBDA_FUNC_NAME_PREFIX || '';

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
configTable = lambdaPrefix + 'LambdaRedshiftBatchLoadConfig';
batchTable = lambdaPrefix + 'LambdaRedshiftBatches';
batchStatusGSI = lambdaPrefix + 'LambdaRedshiftBatchStatus';
filesTable = lambdaPrefix + 'LambdaRedshiftProcessedFiles';
conditionCheckFailed = 'ConditionalCheckFailedException';
provisionedThroughputExceeded = 'ProvisionedThroughputExceededException';
deployedFunctionName = lambdaPrefix + 'LambdaRedshiftLoader';
INVALID_ARG = -1;
ERROR = -1;
OK = 0;
