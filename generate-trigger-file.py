#!/usr/bin/env python

#   Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
#
#        http://aws.amazon.com/asl/
#
#    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 

import boto
import sys
import tempfile

def main(args):        
    if len(args) != 5:
        sys.stdout.write("generate-trigger-file.py <region> <input bucket> <input prefix> <local directory>\n");
        sys.exit(-1)

    # connect to s3
    s3 = boto.s3.connect_to_region(args[1])

    # get the rest of the required arguments
    inputBucket = args[2]
    prefix = args[3]
    localDir = args[4]
    
    bucket = s3.get_bucket(inputBucket)
    
    # create the dummy file
    filename = "lambda-redshift-trigger-file.dummy";
    f = open(localDir + "/" + filename, 'w')
    f.write("\n")
    f.flush()
    f.close()
    
    # upload the dummy to S3
    f = open(localDir + "/" + filename, 'r')
    key = bucket.new_key(prefix + "/" + filename)
    key.set_contents_from_file(f)
    f.close()
    
    sys.stdout.write("Wrote Dummy Trigger File to " + bucket.name + "/" + key.name);

if __name__ == "__main__":
    main(sys.argv)