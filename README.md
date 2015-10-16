# A Zero Administration AWS Lambda Based Amazon Redshift Database Loader

With this AWS Lambda function, it's never been easier to get file data into Amazon 
Redshift. You simply push files into a variety of locations on Amazon S3, and 
have them automatically loaded into your Amazon Redshift clusters.

For automated delivery of streaming data to S3 and Redshift, also consider using Amazon Kinesis Firehose: https://aws.amazon.com/kinesis/firehose 

## Using AWS Lambda with Amazon Redshift
Amazon Redshift is a fully managed petabyte scale data warehouse available for 
less than $1000/TB/YR that provides AWS customers with an extremely powerful way to 
analyse their applications and business as a whole. To load their Clusters, customers 
ingest data from a large number of sources, whether they are FTP locations managed 
by third parties, or internal applications generating load files. Best practice for 
loading Amazon Redshift is to use the COPY command (http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html), which loads data in parallel from Amazon S3, Amazon DynamoDB or an HDFS file system on Amazon Elastic MapReduce (EMR). 

Whatever the input, customers must run servers that look for new data on the file 
system, and manage the workflow of loading new data and dealing with any issues 
that might arise. That's why we created the AWS Lambda-based Amazon Redshift loader 
(http://github.com/awslabs/aws-lambda-redshift-loader) - it offers you the ability 
drop files into S3 and load them into any number of database tables in multiple 
Amazon Redshift Clusters automatically - with no servers to maintain. This is possible 
because AWS Lambda (http://aws.amazon.com/lambda) provides an event-driven, zero-administration 
compute service. It allows developers to create applications that are automatically 
hosted and scaled, while providing you with a fine-grained pricing structure.

![Loader Architecture](Architecture.png)

The function maintains a list of all the files to be loaded from S3 into Amazon 
Redshift using a DynamoDB table. This list allows us to confirm that a file is loaded 
only one time, and allows you to determine when a file was loaded and into which table. 
Input file locations are buffered up to a specified batch size that you control, or 
you can specify a time-based threshold which triggers a load. 

You can specify any of the many COPY options available, and we support loading 
both CSV files (of any delimiter), as well as JSON files (with or without JSON 
paths specifications). All Passwords and Access Keys are encrypted for security. 
With AWS Lambda you get automatic scaling, high availability, and built in Amazon 
CloudWatch Logging.

Finally, we've provided tools to manage the status of your load processes, with 
built in configuration management and the ability to monitor batch status and 
troubleshoot issues. We also support sending notifications of load status through 
Simple Notification Service - SNS (http://aws.amazon.com/sns), so you have visibility 
into how your loads are progressing over time.

## Getting Access to the AWS Lambda Redshift Database Loader
You can download the AWS Lambda function today from AWSLabs: http://github.com/awslabs/aws-lambda-redshift-loader. For example, perform the following steps to complete local setup:

```
git clone https://github.com/awslabs/aws-lambda-redshift-loader.git
cd aws-lambda-redshift-loader
npm install
```

## Getting Started - Preparing your Amazon Redshift Clusters
In order to load a cluster, we'll have to enable AWS Lambda to connect. To do 
this, we must enable Cluster Security Groups to allow access from the public 
internet.

To configure a cluster security group for access:

1.	Log in to the Amazon Redshift console.
2.	Select Security in the navigation pane on the left.
3.	Choose the cluster security group in which your cluster is configured.
4.	Add a new Connection Type of CIDR/IP and enter the value 0.0.0.0/0.
5.	Select Authorize to save your changes.

We recommend granting Amazon Redshift users only INSERT rights on tables to be 
loaded. Create a user with a complex password using the CREATE USER command 
(http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_USER.html), and grant 
INSERT using GRANT (http://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html). 

## Getting Started - Deploying the AWS Lambda Function
To deploy the function:

1.	Go to the AWS Lambda Console in the same region as your S3 bucket and Amazon Redshift cluster.
2.	Select Create a Lambda function and enter the name MyLambdaDBLoader (for example).
3.	Under Code entry type select Upload a zip file and upload the [AWSLambdaRedshiftLoader-2.1.0.zip](https://github.com/awslabs/aws-lambda-redshift-loader/blob/master/dist/AWSLambdaRedshiftLoader-2.1.0.zip) from the dist folder
4.	Use the default values of index.js for the filename and handler for the handler, and follow the wizard for creating the AWS Lambda Execution Role.  We also recommend using the max timeout for the function to accomodate long COPY times.

Next, configure an event source, which delivers S3 events to your AWS Lambda function.

1.	On the deployed function, select Configure Event Source and select the bucket you want to use for input data. Select either the lambda_invoke_role or use the Create/Select function to create the default invocation role. Ensure that you have selected 'Object Created' or the 'ObjectCreated:*' notification type.
2.	Click Submit to save the changes.

When you're done, you'll see that the AWS Lambda function is deployed and you 
can submit test events and view the CloudWatch Logging log streams.

### A Note on Versions
We previously released version 1.0 in distribution AWSLambdaRedshiftLoader.zip, 
which didn't use the Amazon Key Management Service for encryption. If you've 
previously deployed and used version 1.0 and want to upgrade to version 1.1, 
then you'll need to recreate your configuration by running `node setup.js` and 
reentering the previous values including connection password, symmetric encryption key, and optionally an S3 Secret Key. 
You'll also need to upgrade the IAM policy for the Lambda Execution Role as listed 
below, as it now requires permissions to talk to the Key Management Service.

Furthermore, version 2.0.0 adds support for loading multiple Redshift clusters in 
parallel. You can deploy the 2.x versions with a 1.1x configuration, and the 
Lambda function will transparently upgrade your configuration to a 2.x compatible 
format. This uses a loadClusters List type in DynamoDB to track all clusters to 
be loaded.

## Getting Started - Lambda Execution Role
You also need to add an IAM policy as shown below to the role that AWS Lambda 
uses when it runs. Once your function is deployed, add the following policy to 
the `LambdaExecRole` to enable AWS Lambda to call SNS, use DynamoDB, write Manifest 
files to S3, perform encryption with the AWS Key Management Service, and pass STS temporary
credentials to Redshift for the COPY command:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Stmt1424787824000",
            "Effect": "Allow",
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
                "sns:Unsubscribe",
                "s3:Get*",
                "s3:Put*",
                "s3:List*",
                "kms:Decrypt",
                "kms:DescribeKey",
                "kms:GetKeyPolicy"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}
```

## Getting Started - Support for Notifications
This function can send notifications on completion of batch processing. Using SNS, 
you can then receive notifications through email and HTTP Push to an application, 
or put them into a queue for later processing. You can even invoke additional Lambda
functions to complete your data load workflow using an SNS Event Source for another
AWS Lambda function. If you would like to receive SNS notifications for succeeded 
loads, failed loads, or both, create SNS Topics and take note of their ID's in the 
form of Amazon Resource Notations (ARN). 

## Getting Started - Entering the Configuration
Now that your function is deployed, we need to create a configuration which tells 
it how and if files should be loaded from S3. Simply install AWS SDK for Javascript 
and configure it with credentials as outlined at http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-intro.html and http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html. You'll also need a local instance of Node.js and to install dependencies using the following command:

`cd aws-lambda-redshift-loader && npm install`

In order to ensure communication with the correct AWS Region, you'll need to set 
an environment variable ```AWS_REGION``` to the desired location. For example, 
for US East use 'us-east=1', and for EU West 1 use 'eu-west-1'.

```export AWS_REGION=eu-central-1``` 

Next, run the setup.js script by entering node setup.js. The script asks questions 
about how the load should be done, including those outlined in the setup appendix 
as the end of this document. 

All data used to manage the lifecycle of data loads is stored in DynamoDB, and 
the setup script automatically provisions the following tables:

* LambdaRedshiftBatchLoadConfig - Stores the configuration of how files in an S3 input prefix should be loaded into Amazon Redshift.
* LambdaRedshiftBatches - Stores the list of all historical and open batches that have been created. There will always be one open batch, and may be multiple closed batches per S3 input prefix from LambdaRedshiftBatchLoadConfig.
* LambdaRedshiftProcessedFiles - Stores the list of all files entered into a batch, which is also used for deduplication of input files.

*** IMPORTANT ***
The tables used by this function are created with a max read & write per-second rate
of 5. This means that you will be able to accommodate 5 concurrent file uploads
per second being managed by ALL input locations which are event sources to this
Lambda function. If you require more than 5 concurrent invocations/second, then 
you MUST increase the Read IOPS on the LambdaRedshiftBatchLoadConfig table, and
the Write IOPS on LambdaRedshiftBatches and LambdaRedshiftProcessedFiles to the 
maximum number of files to be concurrently processed by all Configurations.

# Security
The database password, as well as the a master symmetric key used for encryption 
will be encrypted by the Amazon Key Management Service (https://aws.amazon.com/kms). This encryption is done with a KMS  
Customer Master Key with an alias named `alias/LambaRedshiftLoaderKey`.

When the Redshift COPY command is created, by default the Lambda function will use a
temporary STS token as credentials for Redshift to use when accessing S3. You can also optionally configure
an Access Key and Secret Key which will be used instead, and
the setup utility will encrypt the secret key.

## Loading multiple Redshift Clusters concurrently
Version 2.0.0 adds the ability to load multiple clusters at the same time. To 
configure an additional cluster, you must first have deployed the 
```AWSLambdaRedshiftLoader-2.1.0.zip``` and had your configuration upgraded to 2.x 
format (you will see a new loadClusters List type in your configuration). You 
can then use the ```addAdditionalClusterEndpoint.js``` to add new clusters into 
a single configuration. This will require you enter the vital details for the 
cluster including endpoint address and port, DB name and password.

You are now ready to go. Simply place files that meet the configured format into 
S3 at the location that you configured as the input location, and watch as AWS 
Lambda loads them into your Amazon Redshift Cluster. You are charged by the number 
of input files that are processed, plus a small charge for DynamoDB. You now have 
a highly available load framework which doesn't require you manage servers!

## Viewing Previous Batches & Status
If you ever need to see what happened to batch loads into your Cluster, you can 
use the 'queryBatches.js' script to look into the LambdaRedshiftBatches DynamoDB 
table. It takes 3 arguments:

* region - the region in which the AWS Lambda function is deployed
* status - the status you are querying for, including 'error', 'complete', 'pending', or 'locked'
* date - optional date argument to use as a start date for querying batches

Running `node queryBatches.js eu-west-1 error` would return a list of all batches 
with a status of 'error' in the EU (Ireland) region, such as:

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

* region - the region in which the AWS Lambda function is deployed
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
We'll only load a file one time by default, but in certain rare cases you might 
want to re-process a file, such as if a batch goes into error state for some reason. 
If so, use the 'processedFiles.js' script to query or delete processed files entries. 
The script takes an 'operation type' and 'filename' as arguments; use -q to query 
if a file has been processed, and -d to delete a given file entry. An example of 
the processed files store can be seen below:

![Processed Files Table](ProcessedFilesTable.png)
 
## Reprocessing a Batch
If you ever need to reprocess a batch - for example if it failed to load the required 
files for some reason - then you can use the reprocessBatch.js script. This takes 
the same arguments as describeBatch.js (region, batch ID & input location). The 
original input batch is not affected; instead, each of the input files that were 
part of the batch are removed from the LambdaRedshiftProcessedFiles table, and 
then the script forces an S3 event to be generated for the file. This will be 
captured and reprocessed by the function as it was originally. Please note you 
can only reprocess batches that are not in 'open' status.

## Unlocking a Batch
It is possible, but rare, that a batch would become locked but not be being processed 
by AWS Lambda. If this were to happen, please use ```unlockBatch.js``` including 
the region and Batch ID to set the batch to 'open' state again.

## Changing your stored Database Password or S3 Secret Key Information
Currently you must edit the configuration manually in Dynamo DB to make changes.
If you need to update your Redshift DB Password, or your Secret Key for allowing
Redshift to access S3, then you can use the ```encryptValue.js``` script to encrypt
a value using the Lambda Redshift Loader master key and encryption context. 

To run:
```
node encryptValue.js <region> <Value to Encrypt>
```

This script encrypts the value with Amazon KMS, and then verifies the encryption is
correct before returning a JSON object which includes the input value and the
encrypted Ciphertext. You can use the 'encryptedCiphertext' attribute of this object
to update the Dynamo DB Configuration. 

## Ensuring Loads happen every N minutes
If you have a prefix that doesn't receive files very often, and want to ensure 
that files are loaded every N minutes, use the following process to force periodic loads. 

When you create the configuration, add a filenameFilterRegex such as '.*\.csv', which 
only loads CSV files that are put into the specified S3 prefix. Then every N minutes, 
schedule the included dummy file generator through a CRON Job. 

```./path/to/function/dir/generate-dummy-file.py <region> <input bucket> <input prefix> <local working directory>```

* region - the region in which the input bucket for loads resides
* input bucket - the bucket which is configured as an input location
* input prefix - the prefix which is configured as an input location
* local working directory - the location where the stub dummy file will be kept prior to upload into S3

This writes a file called 'lambda-redshift-trigger-file.dummy' to the configured 
input prefix, which causes your deployed function to scan the open pending batch 
and load the contents if the timeout seconds limit has been reached.

## Reviewing Logs
For normal operation, you won't have to do anything from an administration perspective. 
Files placed into the configured S3 locations will be loaded when the number of 
new files equals the configured batch size. You may want to create an operational 
process to deal with failure notifications, but you can also just view the performance 
of your loader by looking at Amazon CloudWatch. Open the CloudWatch console, and 
then click 'Logs' in the lefthand navigation pane. You can then select the log 
group for your function, with a name such as `/aws/lambda/<My Function>`.

Each of the above Log Streams were created by an AWS Lambda function invocation, 
and will be rotated periodically. You can see the last ingestion time, which is 
when AWS Lambda last pushed events into CloudWatch Logging.

You can then review each log stream, and see events where your function simply 
buffered a file, or where it performed a load.
 
## Extending and Building New Features
We're excited to offer this AWS Lambda function under the Amazon Software License. 
The GitHub repository does not include all the dependencies for Node.js, so in 
order to build and run locally please install the following modules with npm install:

* Node Postgres - Native Postgres Driver for Javascript (https://github.com/brianc/node-postgres & `npm install pg`)
* Async - Higher-order functions and common patterns for asynchronous code (https://www.npmjs.com/package/async & `npm install async`)
* Node UUID - Rigorous implementation of RFC4122 (v1 and v4) UUIDs (https://www.npmjs.com/package/node-uuid & `npm install node-uuid`)

# Configuration Reference

The following section provides guidance on the configuration options supported. 
For items such as the batch size, please keep in mind that in Preview the Lambda 
function timeout is 60 seconds. This means that your COPY command must complete 
in less than ~ 50 seconds so that the Lambda function has time to complete writing 
batch metadata. The COPY time will be a function of file size, the number of files 
to be loaded, the size of the cluster, and how many other processes might be consuming 
WorkLoadManagement queue slots.

Item | Required | Notes
:---- | :--------: | :-----
Enter the Region for the Redshift Load Configuration| Y | Any AWS Region from http://docs.aws.amazon.com/general/latest/gr/rande.html, using the short name (for example us-east-1 for US East 1)
Enter the S3 Bucket & Prefix to watch for files | Y | An S3 Path in format <bucket name>/<prefix>. Prefix is optional
Enter a Filename Filter Regex | N | A Regular Expression used to filter files which appeared in the input prefix before they are processed.
Enter the Cluster Endpoint | Y | The Amazon Redshift Endpoint Address for the Cluster to be loaded.
Enter the Cluster Port | Y | The port on which you have configured your Amazon Redshift Cluster to run.
Enter the Database Name | Y | The database name in which the target table resides.
Enter the Database Username | Y | The username which should be used to connect to perform the COPY. Please note that only table owners can perform COPY, so this should be the schema in which the target table resides.
Enter the Database Password | Y | The password for the database user. Will be encrypted before storage in Dynamo DB.
Enter the Table to be Loaded | Y | The Table Name to be loaded with the input data.
Should the Table be Truncated before Load? (Y/N) | N | Option to truncate the table prior to loading. Use this option if you will subsequently process the input patch and only want to see 'new' data with this ELT process.
Enter the Data Format (CSV or JSON) | Y | Whether the data format is Character Separated Values or JSON data (http://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html).
If CSV, Enter the CSV Delimiter | Yes if Data Format = CSV | Single character delimiter value, such as ',' (comma) or '|' (pipe).
If JSON, Enter the JSON Paths File Location on S3 (or NULL for Auto) | Yes if Data Format = JSON | Location of the JSON paths file to use to map the file attributes to the database table. If not filled, the COPY command uses option 'json = auto' and the file attributes must have the same name as the column names in the target table.
Enter the S3 Bucket for Redshift COPY Manifests | Y | The S3 Bucket in which to store the manifest files used to perform the COPY. Should not be the input location for the load.
Enter the Prefix for Redshift COPY Manifests| Y | The prefix for COPY manifests.
Enter the Prefix to use for Failed Load Manifest Storage | N | On failure of a COPY, you can elect to have the manifest file copied to an alternative location. Enter that prefix, which will be in the same bucket as the rest of your COPY manifests.
Enter the Access Key used by Redshift to get data from S3. If NULL then Lambda execution role credentials will be used. | N | Amazon Redshift must provide credentials to S3 to be allowed to read data. Enter the Access Key for the Account or IAM user that Amazon Redshift should use.
Enter the Secret Key used by Redshift to get data from S3. If NULL then Lambda execution role credentials will be used. | N | The Secret Key for the Access Key used to get data from S3. Will be encrypted prior to storage in DynamoDB.
Enter the SNS Topic ARN for Failed Loads | N | If you want notifications to be sent to an SNS Topic on successful Load, enter the ARN here. This would be in format 'arn:aws:sns:<region>:<account number>:<topic name>.
Enter the SNS Topic ARN for Successful Loads  | N | SNS Topic ARN for notifications when a batch COPY fails.
How many files should be buffered before loading? | Y | Enter the number of files placed into the input location before a COPY of the current open batch should be performed. Recommended to be an even multiple of the number of CPU's in your cluster. You should set the multiple such that this count causes loads to be every 2-5 minutes.
How old should we allow a Batch to be before loading (seconds)? | N | AWS Lambda will attempt to sweep out 'old' batches using this value as the number of seconds old a batch can be before loading. This 'sweep' is on every S3 event on the input location, regardless of whether it matches the Filename Filter Regex. Not recommended to be below 120.
Additional Copy Options to be added | N | Enter any additional COPY options that you would like to use, as outlined at (http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html). Please also see http://blogs.aws.amazon.com/bigdata/post/Tx2ANLN1PGELDJU/Best-Practices-for-Micro-Batch-Loading-on-Amazon-Redshift for information on good practices for COPY options in high frequency load environments.

----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

	http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.