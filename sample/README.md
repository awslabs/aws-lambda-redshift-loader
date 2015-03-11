This sample includes the setup scripts to configure your database for loads of the sample data, as well as the script to create a sample configuration.

To get started, deploy the AWSLambdaRedshiftLoader-1.1.zip from the ‘dist’ folder as outlined in the Getting Started section, and install the dependent modules (npm install). You’ll also need to have an Amazon Redshift cluster set up, and have the cluster endpoint address, port, the database name in which you want to run the sample, and the Username & Password of a database user that can create a user specifically used for the sample. You’ll also need to have the Postgres command line client and a bash terminal.

Once you are ready, just run the configureSample.sh in the sample/scripts directory using a Terminal program such as PuTTY. This requires arguments of the cluster endpoint address, port, db name, db user, and AWS region to be used (in that order), and will prompt for your DB user password. This script then:
* Creates a database user called test_lambda_load_user, which you can drop after you are finished with the sample
* Creates a database table owned by this new user called lambda_redshift_sample, which just has three integer columns
* Runs the configuration script which will further prompt for required configuration values such as the S3 bucket you want to use for the sample, and access key information

You are now set to try out loading the database. Simply transfer the files from the sample/data directory to the input prefix in the S3 bucket you provided to the setup script. For example (using the AWS CLI):

```aws s3 sync ../data s3://<my bucket>/input --region <region for my bucket>```

You can then go into your deployed AWS Lambda function and review the CloudWatch Log Streams which will show two loaded batches of two files each, and one file in an open batch.

To clean up the demo loader, just run cleanup.sh with the same arguments. The Amazon Redshift table and user will be deleted and the configuration tables in DynamoDB will also be removed.
