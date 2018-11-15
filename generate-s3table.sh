#!/bin/bash
#set -x

ver=`cat package.json | grep "version" | cut -d: -f2 | sed -e "s/[\"\,]//g" | tr -d '[:space:]'`
token=AWSLambdaRedshiftLoader

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do
    link=s3://awslabs-code-$r/LambdaRedshiftLoader/$token-$ver.zip
    echo "| $r | [$link]($link)"
done