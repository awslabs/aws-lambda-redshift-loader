#!/bin/bash
#set -x

ver=$1
token=AWSLambdaRedshiftLoader

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do
    aws s3 rm s3://awslabs-code-$r/LambdaRedshiftLoader/$token-$ver.zip --region $r;
done
