#!/bin/bash

if [ $# -ne 4 ]; then
  echo "You must provide the cluster Endpoint, Port, and DB Name and Database Master Username";
  exit -1;
fi

endpoint=$1
port=$2
db=$3
user_name=$4

# export these variables for use by the createSampleConfig.js
export CLUSTER_ENDPOINT=$endpoint
export CLUSTER_PORT=$port
export CLUSTER_DB=$db

# create the sample database user
psql -U $user_name -h $endpoint -p $port -f createRedshiftUser.sql -d $db -a

export PGPASSWORD=Change-me1!

# create the db table
psql -U test_lambda_load_user -h $endpoint -p $port -f createRedshiftTable.sql -d $db -a

export PGPASSWORD=

# run the sample configuration setup
node createSampleConfig.js