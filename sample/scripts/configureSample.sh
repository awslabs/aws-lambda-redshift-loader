#!/bin/bash

if [ $# -ne 5 ]; then
  echo "You must provide the cluster Endpoint, Port, DB Name, Database Master Username and region";
  exit -1;
fi

endpoint=$1
port=$2
db=$3
user_name=$4
region=$5

# export these variables for use by the createSampleConfig.js
export CLUSTER_ENDPOINT=$endpoint
export CLUSTER_PORT=$port
export CLUSTER_DB=$db
export AWS_REGION=$region

# create the sample database user
psql -U $user_name -h $endpoint -p $port -f createRedshiftUser.sql -d $db -a

export PGPASSWORD=Change-me1!

# create the db table
psql -U test_lambda_load_user -h $endpoint -p $port -f createRedshiftTable.sql -d $db -a

export PGPASSWORD=

# run the sample configuration setup
node createSampleConfig.js