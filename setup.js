/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

/**
 * Ask questions of the end user via STDIN and then setup the DynamoDB table
 * entry for the configuration when done
 */
var pjson = require('./package.json');
var readline = require('readline');
var aws = require('aws-sdk');
require('./constants');
var common = require('./common');
var async = require('async');
var uuid = require('uuid');
var dynamoDB;
var s3;
var lambda;
var kmsCrypto = require('./kmsCrypto');
var setRegion;

dynamoConfig = {
    TableName: configTable,
    Item: {
        currentBatch: {
            S: uuid.v4()
        },
        version: {
            S: pjson.version
        },
        loadClusters: {
            L: [{
                M: {}
            }]
        }
    }
};

/* configuration of question prompts and config assignment */
var rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

var qs = [];

q_region = function (callback) {
    rl.question('Enter the Region for the Configuration > ', function (answer) {
        if (common.blank(answer) !== null) {
            setRegion = answer.toLowerCase();

            // configure dynamo db, kms, s3 and lambda for the correct region
            dynamoDB = new aws.DynamoDB({
                apiVersion: '2012-08-10',
                region: setRegion
            });
            kmsCrypto.setRegion(setRegion);
            s3 = new aws.S3({
                apiVersion: '2006-03-01'
            });
            lambda = new aws.Lambda({
                apiVersion: '2015-03-31',
                region: setRegion
            });

            callback(null);
        }
    });
};

q_s3Prefix = function (callback) {
    rl.question('Enter the S3 Bucket & Prefix to watch for files > ', function (answer) {
        common.validateNotNull(answer, 'You Must Provide an S3 Bucket Name, and optionally a Prefix', rl);

        // setup prefix to be * if one was not provided
        var stripped = answer.replace(new RegExp('s3://', 'g'), '');
        var elements = stripped.split("/");
        var setPrefix = undefined;

        if (elements.length === 1) {
            // bucket only so use "bucket" alone
            setPrefix = elements[0];
        } else {
            // right trim "/"
            setPrefix = stripped.replace(/\/$/, '');
        }

        dynamoConfig.Item.s3Prefix = {
            S: setPrefix
        };

        callback(null);
    });
};

q_filenameFilter = function (callback) {
    rl.question('Enter a Filename Filter Regex > ', function (answer) {
        if (common.blank(answer) !== null) {
            dynamoConfig.Item.filenameFilterRegex = {
                S: answer
            };
        }
        callback(null);
    });
};

q_clusterEndpoint = function (callback) {
    rl.question('Enter the Cluster Endpoint > ', function (answer) {
        common.validateNotNull(answer, 'You Must Provide a Cluster Endpoint', rl);
        dynamoConfig.Item.loadClusters.L[0].M.clusterEndpoint = {
            S: answer
        };
        callback(null);
    });
};

q_clusterPort = function (callback) {
    rl.question('Enter the Cluster Port > ', function (answer) {
        dynamoConfig.Item.loadClusters.L[0].M.clusterPort = {
            N: '' + common.getIntValue(answer, rl)
        };
        callback(null);
    });
};

q_clusterUseSSL = function (callback) {
    rl.question('Does your cluster use SSL (Y/N)  > ', function (answer) {
        dynamoConfig.Item.loadClusters.L[0].M.useSSL = {
            BOOL: common.getBooleanValue(answer)
        };
        callback(null);
    });
};

q_clusterDB = function (callback) {
    rl.question('Enter the Database Name > ', function (answer) {
        if (common.blank(answer) !== null) {
            dynamoConfig.Item.loadClusters.L[0].M.clusterDB = {
                S: answer
            };
        }
        callback(null);
    });
};

q_userName = function (callback) {
    rl.question('Enter the Database Username > ', function (answer) {
        common.validateNotNull(answer, 'You Must Provide a Username', rl);
        dynamoConfig.Item.loadClusters.L[0].M.connectUser = {
            S: answer
        };
        callback(null);
    });
};

q_userPwd = function (callback) {
    rl.question('Enter the Database Password (will be displayed, but encrypted before storing) > ', function (answer) {
        common.validateNotNull(answer, 'You Must Provide a Password', rl);

        kmsCrypto.encrypt(answer, function (err, ciphertext) {
            if (err) {
                console.log(JSON.stringify(err));
                process.exit(ERROR);
            } else {
                dynamoConfig.Item.loadClusters.L[0].M.connectPassword = {
                    S: kmsCrypto.toLambdaStringFormat(ciphertext)
                };
                callback(null);
            }
        });
    });
};

q_table = function (callback) {
    rl.question('Enter the Table to be Loaded > ', function (answer) {
        common.validateNotNull(answer, 'You Must Provide a Table Name', rl);
        dynamoConfig.Item.loadClusters.L[0].M.targetTable = {
            S: answer
        };
        callback(null);
    });
};

q_columnList = function (callback) {
    rl.question('Enter the comma-delimited column list (optional) > ', function (answer) {
        if (answer && answer !== null && answer !== "") {
            dynamoConfig.Item.loadClusters.L[0].M.columnList = {
                S: answer
            };
            callback(null);
        } else {
            callback(null);
        }
    });
};

q_truncateTable = function (callback) {
    rl.question('Should the Table be Truncated before Load? (Y/N) > ', function (answer) {
        dynamoConfig.Item.loadClusters.L[0].M.truncateTarget = {
            BOOL: common.getBooleanValue(answer)
        };
        callback(null);
    });
};

q_df = function (callback) {
    rl.question('Enter the Data Format (CSV, JSON, AVRO, Parquet, and ORC) > ', function (answer) {
        common.validateArrayContains(['CSV', 'JSON', 'AVRO', 'Parquet', 'ORC'], answer.toUpperCase(), rl);
        dynamoConfig.Item.dataFormat = {
            S: answer.toUpperCase()
        };
        callback(null);
    });
};

q_csvDelimiter = function (callback) {
    if (dynamoConfig.Item.dataFormat.S === 'CSV') {
        rl.question('Enter the CSV Delimiter > ', function (answer) {
            common.validateNotNull(answer, 'You Must the Delimiter for CSV Input', rl);
            dynamoConfig.Item.csvDelimiter = {
                S: answer
            };
            callback(null);
        });
    } else {
        callback(null);
    }
};

q_ignoreCsvHeader = function (callback) {
    rl.question('ignore Header (first line) of the CSV file? (Y/N) > ', function (answer) {
        dynamoConfig.Item.ignoreCsvHeader = {
            BOOL: common.getBooleanValue(answer)
        };
        callback(null);
    });
};

q_jsonPaths = function (callback) {
    if (dynamoConfig.Item.dataFormat.S === 'JSON' || dynamoConfig.Item.dataFormat.S === 'AVRO') {
        rl.question('Enter the JSON Paths File Location on S3 (or NULL for Auto) > ', function (answer) {
            if (common.blank(answer) !== null) {
                dynamoConfig.Item.jsonPath = {
                    S: answer
                };
            }
            callback(null);
        });
    } else {
        callback(null);
    }
};

q_manifestBucket = function (callback) {
    rl.question('Enter the S3 Bucket for Redshift COPY Manifests > ', function (answer) {
        common.validateNotNull(answer, 'You Must Provide a Bucket Name for Manifest File Storage', rl);
        dynamoConfig.Item.manifestBucket = {
            S: answer
        };
        callback(null);
    });
};

q_manifestPrefix = function (callback) {
    rl.question('Enter the Prefix for Redshift COPY Manifests > ', function (answer) {
        common.validateNotNull(answer, 'You Must Provide a Prefix for Manifests', rl);
        dynamoConfig.Item.manifestKey = {
            S: answer
        };
        callback(null);
    });
};

q_failedManifestPrefix = function (callback) {
    rl.question('Enter the Prefix to use for Failed Load Manifest Storage (must differ from the initial manifest path) > ', function (answer) {
        common.validateNotNull(answer, 'You Must Provide a Prefix for Manifests', rl);
        dynamoConfig.Item.failedManifestKey = {
            S: answer
        };
        callback(null);
    });
};

q_accessKey = function (callback) {
    rl.question('Enter the Access Key used by Redshift to get data from S3. If NULL then Lambda execution role credentials will be used > ', function (answer) {
        if (!answer) {
            callback(null);
        } else {
            dynamoConfig.Item.accessKeyForS3 = {
                S: answer
            };
            callback(null);
        }
    });
};

q_secretKey = function (callback) {
    rl.question('Enter the Secret Key used by Redshift to get data from S3. If NULL then Lambda execution role credentials will be used > ', function (answer) {
        if (!answer) {
            callback(null);
        } else {
            kmsCrypto.encrypt(answer, function (err, ciphertext) {
                if (err) {
                    console.log(JSON.stringify(err));
                    process.exit(ERROR);
                } else {
                    dynamoConfig.Item.secretKeyForS3 = {
                        S: kmsCrypto.toLambdaStringFormat(ciphertext)
                    };
                    callback(null);
                }
            });
        }
    });
};

q_symmetricKey = function (callback) {
    rl.question('If Encrypted Files are used, Enter the Symmetric Master Key Value > ', function (answer) {
        if (answer && answer !== null && answer !== "") {
            kmsCrypto.encrypt(answer, function (err, ciphertext) {
                if (err) {
                    console.log(JSON.stringify(err));
                    process.exit(ERROR);
                } else {
                    dynamoConfig.Item.masterSymmetricKey = {
                        S: kmsCrypto.toLambdaStringFormat(ciphertext)
                    };
                    callback(null);
                }
            });
        } else {
            callback(null);
        }
    });
};

q_failureTopic = function (callback) {
    rl.question('Enter the SNS Topic ARN for Failed Loads > ', function (answer) {
        if (common.blank(answer) !== null) {
            dynamoConfig.Item.failureTopicARN = {
                S: answer
            };
        }
        callback(null);
    });
};

q_successTopic = function (callback) {
    rl.question('Enter the SNS Topic ARN for Successful Loads > ', function (answer) {
        if (common.blank(answer) !== null) {
            dynamoConfig.Item.successTopicARN = {
                S: answer
            };
        }
        callback(null);
    });
};

q_batchSize = function (callback) {
    rl.question('How many files should be buffered before loading? > ', function (answer) {
        if (common.blank(answer) !== null) {
            dynamoConfig.Item.batchSize = {
                N: '' + common.getIntValue(answer, rl)
            };
        }
        callback(null);
    });
};

q_batchTimeoutSecs = function (callback) {
    rl.question('How old should we allow a Batch to be before loading (seconds)? > ', function (answer) {
        if (common.blank(answer) !== null) {
            dynamoConfig.Item.batchTimeoutSecs = {
                N: '' + common.getIntValue(answer, rl)
            };
        }
        callback(null);
    });
};

q_batchBytes = function (callback) {
    rl.question('Batches can be buffered up to a specified size. How large should a batch be before processing (bytes)? > ', function (answer) {
        if (common.blank(answer) !== null) {
            dynamoConfig.Item.batchSizeBytes = {
                N: '' + common.getIntValue(answer, rl)
            };
        }
        callback(null);
    });
};

q_copyOptions = function (callback) {
    rl.question('Additional Copy Options to be added > ', function (answer) {
        if (common.blank(answer) !== null) {
            dynamoConfig.Item.copyOptions = {
                S: answer
            };
        }
        callback(null);
    });
};

last = function (callback) {
    rl.close();

    exports.setup(dynamoConfig, callback);
};

// export the setup module so that customers can programmatically add new
// configurations
setup = function (useConfig, callback) {
    common.setup(useConfig, dynamoDB, s3, lambda, callback);
};
exports.setup = setup;

qs.push(q_region);
qs.push(q_s3Prefix);
qs.push(q_filenameFilter);
qs.push(q_clusterEndpoint);
qs.push(q_clusterPort);
qs.push(q_clusterUseSSL);
qs.push(q_clusterDB);
qs.push(q_table);
qs.push(q_columnList);
qs.push(q_truncateTable);
qs.push(q_userName);
qs.push(q_userPwd);
qs.push(q_df);
qs.push(q_csvDelimiter);
qs.push(q_ignoreCsvHeader);
qs.push(q_jsonPaths);
qs.push(q_manifestBucket);
qs.push(q_manifestPrefix);
qs.push(q_failedManifestPrefix);
qs.push(q_accessKey);
qs.push(q_secretKey);
qs.push(q_successTopic);
qs.push(q_failureTopic);
qs.push(q_batchSize);
qs.push(q_batchBytes);
qs.push(q_batchTimeoutSecs);
qs.push(q_copyOptions);
qs.push(q_symmetricKey);

// always have to have the 'last' function added to halt the readline channel
// and run the setup
qs.push(last);

// call the first function in the function list, to invoke the callback
// reference chain
async.waterfall(qs);
