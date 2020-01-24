#!/bin/bash

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do echo "|$r |  [<img src=\"https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png\" target=\”_blank\”>](https://console.aws.amazon.com/cloudformation/home?region=$r#/stacks/new?stackName=LambdaRedshiftLoader&templateURL=https://awslabs-code-$r.s3.amazonaws.com/LambdaRedshiftLoader/deploy.yaml) |[<img src=\"https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png\" target=\”_blank\”>](https://console.aws.amazon.com/cloudformation/home?region=$r#/stacks/new?stackName=LambdaRedshiftLoaderVPC&templateURL=https://awslabs-code-$r.s3.amazonaws.com/LambdaRedshiftLoader/deploy-non-vpc.yaml) |"; done

