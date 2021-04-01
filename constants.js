/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

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
configTable = process.env.CONFIG_TABLE_NAME ? process.env.CONFIG_TABLE_NAME : 'LambdaRedshiftBatchLoadConfig';
batchTable =  process.env.BATCH_TABLE_NAME ? process.env.BATCH_TABLE_NAME :'LambdaRedshiftBatches';
batchStatusGSI = 'LambdaRedshiftBatchStatus';
filesTable =  process.env.PROCESSED_FILES_TABLE_NAME ? process.env.PROCESSED_FILES_TABLE_NAME :'LambdaRedshiftProcessedFiles';
conditionCheckFailed = 'ConditionalCheckFailedException';
provisionedThroughputExceeded = 'ProvisionedThroughputExceededException';
deployedFunctionName = 'LambdaRedshiftLoader';
INVALID_ARG = -1;
ERROR = -1;
OK = 0;
SUPPRESS_FAILURE_ON_OK_NOTIFICATION = 'SuppressFailureStatusOnSuccessfulNotification'
