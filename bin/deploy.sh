#!/bin/bash
#set -x

ver=`cat package.json | grep "version" | cut -d: -f2 | sed -e "s/[\"\,]//g" | tr -d '[:space:]'`
token=AWSLambdaRedshiftLoader

echo "Deploying Lambda Redshift Loader $ver to AWSLabs S3 Buckets"

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do
    # publish the build
    aws s3 cp dist/$token-$ver.zip s3://awslabs-code-$r/LambdaRedshiftLoader/$token-$ver.zip --acl public-read --region $r;

    # publish deploy.yaml to regional buckets
    aws s3 cp deploy.yaml s3://awslabs-code-$r/LambdaRedshiftLoader/deploy.yaml --acl public-read --region $r;

    aws s3 cp deploy-vpc.yaml s3://awslabs-code-$r/LambdaRedshiftLoader/deploy-vpc.yaml --acl public-read --region $r;

    aws s3 cp deploy-admin-host.yaml s3://awslabs-code-$r/LambdaRedshiftLoader/deploy-admin-host.yaml --acl public-read --region $r;
done
