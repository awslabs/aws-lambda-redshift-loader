# AWS Lambda based Redshift Database Loader

Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl

----

With this Lambda function, it’s never been easier to get file data into Amazon 
Redshift. You simply push files into a variety of locations on Amazon S3, and 
have them automatically loaded into your Amazon Redshift clusters. 

# Using AWS Lambda with Amazon Redshift

Amazon Redshift is a fully managed petabyte scale data warehouse available for 
less than $1000/TB/YR, and provides AWS customers with an extremely powerful way 
to analyse their applications and business as a whole. To load their Clusters, 
customers will be ingesting data from a large number of sources, whether they 
are FTP locations managed by third parties, or internal applications generating 
load files. Best practice for loading Redshift is to use the COPY command 
(http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html), which loads data in 
parallel from Amazon S3, Amazon DynamoDB or an HDFS file system on Amazon Elastic 
MapReduce. Whatever the input, customers must run servers that look for new data 
on the file system, and manage the workflow of loading new data and dealing with 
any issues that might arise. That’s why we created Amazon (http://github.com/awslabs/aws-lambda-redshift-loader) - 
it offers you the ability drop files into S3 and load them into any number of 
database tables in multiple Amazon Redshift Clusters automatically - with no 
servers to maintain. This is possible because AWS Lambda(http://aws.amazon.com/lambda), 
provides an event-driven, zero-administration compute platform for back-end services. 
It allows developers to create applications that are automatically hosted and scaled, 
while providing you with a fine-grained pricing structure.

The function maintains a list of all the files to be loaded from S3 into an Amazon 
Redshift Cluster using Amazon DynamoDB. This list allows us to confirm that a file 
is loaded only once, and allows you to determine when a file was loaded and into 
which table. Input file locations are buffered up to a specified batch size that 
you control, or you can specify a time-based threshold which triggers a load. 
You can specify any of the many COPY options available, and we supports loading 
both CSV files (of any delimiter), as well as JSON files (with or without JSON 
Paths specifications). All Passwords and Access Keys are encrypted for security. 
Finally, with AWS Lambda you get automatic scaling, high availability, and built 
in CloudWatch Logging.

Finally, we’ve provided tools to manage the status of your load processes, with 
built in configuration management and the ability to monitor batch status and 
troubleshoot issues. We also support sending notifications of load status through 
Simple Notification Service (http://aws.amazon.com/sns), so you have visibility 
into how your loads are progressing over time.

## Getting Started - Preparing your Redshift Cluster

In order to load a cluster, we’ll have to enable AWS Lambda to connect. To do 
this, we must enable the Cluster Security Group to allow access from the public 
internet. In the future AWS Lambda will support presenting Lambda as though it 
was inside your own VPC. To configure your Cluster Security Group for access, go 
to the Amazon Web Services Web Console, log in, and select ‘Redshift’ from the 
list of services. On the left hand navigation menu, select ‘Security’, and then 
choose the Cluster Security Group in which your Cluster is configured. At the 
bottom, add a new ‘Connection Type’ of CIDR/IP, and enter value 0.0.0.0/0. Then 
select Authorize to save your changes.

We recommend granting Amazon Redshift users only INSERT rights on tables to be 
loaded. Create a user with a complex password using the ‘CREATE USER’ command 
(http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_USER.html), and grant INSERT 
using GRANT (http://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html). 

## Getting Started - Deploying the AWS Lambda Function

To deploy the function, go to the Amazon Lambda Console in the same region as your 
Amazon S3 bucket and Amazon Redshift cluster. Select ‘Create a Lambda function’, 
and enter name ‘MyLambdaDBLoader’ (for example). Under ‘Code entry type’ select 
‘Upload a zip file’, and then and upload the AWSLambdaRedshiftLoader.zip from 
GitHub. Use the default values of 'index.js' for the filename, and 'handler' for 
the handler, and follow the wizard for creating the Lambda Execution Role.  We 
also recommend using the max timeout for the function, which in preview is 60 seconds. 

Next, configure an Event Source, which will deliver S3 PUT events to your Lambda 
function. On the deployed function, select ‘Configure Event Source’ and then select 
the bucket you want to use for Input Data, and either select the ‘lambda_invoke_role’, 
or use the ‘Create/Select’ function to create the default invocation role. Press 
Submit to save the changes. When done, you’ll see that the Lambda function is deployed 
and you can submit test events as well as view the CloudWatch Logging log streams created.

## Getting Started - Lambda Execution Role

You will also need to add an IAM Policy as shown below to the Role that AWS Lambda 
uses when it runs. Once your function is deployed, add the below policy to the 
`lambda_exec_role` to enable Lambda to call the SNS Service and use DynamoDB:

```
{
  "Version": "some-unique-version-information",
  "Statement": [
    {
      "Action": [
        "dynamodb:DeleteItem",
        "dynamodb:DescribeTable",
        "dynamodb:GetItem",
        "dynamodb:ListTables",
        "dynamodb:PutItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:UpdateItem",
        "sns:GetEndpointAttributes",
        "sns:GetSubscriptionAttributes",
        "sns:GetTopicAttributes",
        "sns:ListTopics",
        "sns:Publish",
        "sns:Subscribe",
        "sns:Unsubscribe"
      ],
      "Effect": "Allow",
      "Resource": "*"
    }
  ]
}
```

## Getting Started - Support for Notifications

This function can send notifications on completion of Batch processing if required. 
Using the Simple Notification Service, you can then receive notifications through 
email and HTTP Push to an application, or put them into a queue for later processing. 
If you would like to receive SNS notifications for succeeded loads, failed loads, 
or both, create SNS Topics and take note of their ID’s in the form of Amazon 
Resource Notations (ARN). 

## Getting Started - Entering the Configuration

Now that the your function is deployed, we need to create a configuration which 
tells it how and if files should be loaded from S3. Simply install AWS Javascript 
SDK and configure it with credentials as outlined at http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-intro.html 
and http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html. You’ll 
also need a local instance of Node.js and to install dependencies using the following 
command:

`cd aws-lambda-redshift-loader && npm install`

Next, run the setup.js script by entering ‘node setup.js’, which will ask a series 
of questions about how the load should be done. Please note that the Database Password, 
as well as the Secret Key used by Redshift to access S3 will be encrypted prior 
to storage in DynamoDB. If you want to change the encryption key, just update the 
`password` value in node_modules/lollyrock/index.js. An example configuration in 
the DynamoDB Console is shown below: 

All data used to manage the lifecycle of data loads is stored in DynamoDB, and 
the setup script will automatically provision the following tables:

* LambdaRedshiftBatchLoadConfig - Stores the configuration of how files in an S3 input prefix should be loaded into Amazon Redshift
* LambdaRedshiftBatches - Stores the list of all historical and open batches that have been created. There will always be one open batch, and may be multiple closed batches per S3 input prefix from LambdaRedshiftBatchLoadConfig
* LambdaRedshiftProcessedFiles - Stores the list of all files entered into a batch, which is also used for deduplication of input files.

You are now ready to go. Simply place files which meet the configured format into 
S3 at the location that you configured as the input location, and watch as your 
Lambda function loads them into your Amazon Redshift Cluster. You’ll be charged 
by the number of input files that are processed, plus a small charge for DynamoDB. 
You now have a highly available load framework which doesn’t require you manage servers!

## Viewing Previous Batches & Status

If you ever need to see what happened to batch loads into your Cluster, you can 
use the 'queryBatches.js' script to look into the LambdaRedshiftBatches DynamoDB 
table. It takes 3 arguments:

* region - the Region in which the Lambda function is deployed
* status - the Status you are querying for, including 'error', 'complete', 'pending', or 'locked'
* date - optional date argument to use as a start date for querying batches

Running `node queryBatches.js eu-west-1 error` would return a list of all errored 
batches in the EU West region, such as:

```
[
    {
        "s3Prefix": "lambda-redshift-loader-test/input",
        "batchId": "2588cc35-b52f-4408-af89-19e53f4acc11",
        "lastUpdateDate": "2015-02-26-16:50:18"
    },
    {
        "s3Prefix": "lambda-redshift-loader-test/input",
        "batchId": "2940888d-146c-47ff-809c-f5fa5d093814",
        "lastUpdateDate": "2015-02-26-16:50:18"
    }
]
```

If you require more detail on a specific batch, you can use describeBatch.js to 
show all detail for a batch. It takes 3 arguments as well:

* region - the Region in which the Lambda function is deployed
* batchId - the batch you would like to see the detail for
* s3Prefix - the S3 Prefix the batch was created for

Which would return the batch information as it is stored in Dynamo DB:

```
{
    "batchId": {
        "S": "7325a064-f67e-416a-acca-17965bea9807"
    },
    "manifestFile": {
        "S": "my-bucket/manifest/manifest-2015-02-06-16:20:20-2081"
    },
    "s3Prefix": {
        "S": "input"
    },
    "entries": {
        "SS": [
            "input/sample-redshift-file-for-lambda-loader.csv",
            "input/sample-redshift-file-for-lambda-loader1.csv",
            "input/sample-redshift-file-for-lambda-loader2.csv",
            "input/sample-redshift-file-for-lambda-loader3.csv",
            "input/sample-redshift-file-for-lambda-loader4.csv",
            "input/sample-redshift-file-for-lambda-loader5.csv"
        ]
    },
    "lastUpdate": {
        "N": "1423239626.707"
    },
    "status": {
        "S": "complete"
    }
}
```

## Clearing Processed Files

We’ll only load a file once by default, but in certain rare cases you might want 
to re-process a file, like if a batch goes into error state for some reason. If so, 
use the 'processedFiles.js' script to query or delete processed files entries. 
The script takes an 'operation type' and 'filename' as arguments; use -q to query 
if a file has been processed, and -d to delete a given file entry.
 
## Reprocessing a Batch

If you ever need to reprocess a batch, for example if it failed to load the required 
files for some reason, then you can use the reprocessBatch.js script. This takes 
the same arguments as describeBatch.js (region, batch ID & input location). The 
original input batch is not affected, and instead each of the input files that was 
part of the batch is removed from the LambdaRedshiftProcessedFiles table, and then 
the script forces an S3 event to be generated for the file. This will be captured and 
reprocessed by the function as it was originally. Please note you can only reprocess 
batches which are not in “open” status.

## Ensuring Periodic Loads

If you have a prefix that doesn't receive files very often, and want to ensure 
that files are loaded every N minutes, use the following process to force periodic 
loads. When you create the configuration, add a filenameFilterRegex such as '.*\.csv', 
which will only load CSV files that are put into the specified S3 prefix. Then every 
N minutes, schedule the included dummy file generator through a CRON Job. 

`./path/to/function/dir/generate-dummy-file.py <region> <input bucket> <input prefix> <local working directory>`

* region - the region in which the input bucket for loads resides
* input bucket - the bucket which is configured as an input location
* input prefix - the prefix which is configured as an input location
* local working directory - the location where the stub dummy file will be kept prior to upload into S3

This will write a file called ‘lambda-redshift-trigger-file.dummy’ to the configured 
input prefix, which will cause your deployed function to scan the open pending 
batch and load the contents if the timeout seconds has been reached.

## Reviewing Logs

Under normal operations, you won’t have to do anything from an administration 
perspective. Files which are placed into the configured S3 locations will be 
loaded when the number of new files equals the configured batch size. You will 
want to create an operational process in case of failure notifications, but you 
can also just view the performance of your loader by looking at Amazon CloudWatch 
Logging. To view, go into the AWS CloudWatch service in the AWS Console, and then 
click ‘Logs’ in the left hand Navigation. You can then select the Log Group for 
your function, which will have a name such as `/aws/lambda/<My Function>`.

Each of the Log Streams were created by a Lambda function invocation, and will 
be rotated periodically. You can see the last ingestion time, which is when AWS 
Lambda last pushed events into CloudWatch Logging.

You can then review each log stream, and see events where your function simply 
buffered a file, or where it performed a load.
 
## Extending and Building New Features

We’re excited to offer this Lambda function under the Amazon Software License. 
The GitHub repository does not include all the dependencies for Node.js, so in 
order to build and run locally please install the following modules with npm install:

* Java - Bridge API to connect with existing Java APIs (https://www.npmjs.com/package/java & `npm install java`) - requires a default path entry for libjvm.so and an installed ‘javac’
* JDBC - Node Module JDBC wrapper (https://www.npmjs.com/package/jdbc & `npm install jdbc`)
* Async - Higher-order functions and common patterns for asynchronous code (https://www.npmjs.com/package/async & `npm install async`)
* Node UUID - Rigorous implementation of RFC4122 (v1 and v4) UUIDs (https://www.npmjs.com/package/node-uuid & `npm install node-uuid`)
 
# Appendix: A Demonstration Loader

In the project we’ve included a ‘sample’ directory which will help you give this 
function a try. This sample includes the setup scripts to configure your database 
for loads of the sample data, as well as the script to create a sample configuration.

To get started, deploy the AWSLambdaRedshiftLoader.zip from the GitHub ‘dist’ folder 
as outlined in the Getting Started section, and install the dependent modules
 (npm install java jdbc async node-uuid). You’ll also need to have a Redshift cluster 
 set up, and have the cluster endpoint address, port, the database name in which 
 you want to run the sample, and the Username & Password of a database user that 
 can create a user specifically used for the sample. You’ll also need to have the 
 Postgres command line client and a bash terminal.

Once you are ready, just run the configureSample.sh in the ‘sample/scripts’ directory. 
This requires arguments of the cluster endpoint address, port, db name and db user, 
in that order, and will prompt for your DB User Password. This script will then:

* Create a database user called test_lambda_load_user, which you can drop after you are finished with the sample
* Create a database table owned by this new user called lambda_redshift_sample, which just has 3 integer columns
* Run the configuration script which will further prompt for required configuration values such as the S3 bucket you want to use for the sample, and access key information

You are now setup to try out loading the database. Simply transfer the files from 
the ‘sample/data’ directory to the ‘input’ prefix in the S3 bucket you provided 
to the setup script. For example (using the AWS CLI):

`aws s3 sync ../data s3://<my bucket>/input --region <region for my bucket>`

You can then go into your deployed Lambda function and review the CloudWatch Log 
Streams which will show 2 loaded batches of 2 files each, and 1 file in an open batch.

To clean up the demo loader, just run ‘cleanup.sh’ with the same arguments, and 
the Redshift table and user will be deleted, and the configuration tables in DynamoDB 
will also be removed.

----

Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl
