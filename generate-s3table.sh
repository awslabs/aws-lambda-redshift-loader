#!/bin/bash
#set -x

ver=`cat package.json | grep "version" | cut -d: -f2 | sed -e "s/[\"\,]//g" | tr -d '[:space:]'`
token=AWSLambdaRedshiftLoader
project_prefix=LambdaRedshiftLoader

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do
    if [ "$r" == "us-east-1" ]; then
        region="."
    else
        region=".$r."
    fi
    link=https://s3$regionamazonaws.com/awslabs-code-$r/$project_prefix/$token-$ver.zip
    echo "| $r | [s3://awslabs-code-$r/$project_prefix/$token-$ver.zip]($link)"
done