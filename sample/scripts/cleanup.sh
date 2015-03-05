#!/bin/bash

if [ $# -ne 4 ]; then
  echo "You must provide the cluster Endpoint, Port, and DB Name and Database Master Username";
  exit -1;
fi

endpoint=$1
port=$2
db=$3
user_name=$4

export PGPASSWORD=Change-me1!

# drop the db table
psql -U test_lambda_load_user -h $endpoint -p $port -f dropRedshiftTable.sql -d $db -a

export PGPASSWORD=

# drop the sample database user
psql -U $user_name -h $endpoint -p $port -f dropRedshiftUser.sql -d $db -a

# drop dynamo tables
node dropSample.js