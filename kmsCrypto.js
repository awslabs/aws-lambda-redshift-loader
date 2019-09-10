/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */
var useRegion = undefined;
var aws = require('aws-sdk');
var async = require('async');
var kms = undefined;

var authContext = {
    module : "AWSLambdaRedshiftLoader",
    region : null
};

// module key alias to be used for this application
var moduleKeyName = "alias/LambdaRedshiftLoaderKey";

var setRegion = function(region) {
    if (!region) {
	useRegion = process.env['AWS_REGION'];

	if (!useRegion) {
	    useRegion = 'us-east-1';
	    console.log("AWS KMS using default region " + useRegion);
	}
    } else {
	useRegion = region;
    }

    aws.config.update({
	region : useRegion
    });
    kms = new aws.KMS({
	apiVersion : '2014-11-01',
	region : useRegion
    });
    authContext.region = useRegion;
};
exports.setRegion = setRegion;

/**
 * Retrieves or creates the master key metadata for this module <br>
 * Parameters:
 * <li>callback(err,KeyMetadata) err - errors generated while getting or
 * creating the key</li>
 * <li>KeyMetadata - KMS Key Metadata including ID and ARN for this module's
 * master key</li>
 */
var getOrCreateMasterKey = function(callback) {
    kms.describeKey({
	KeyId : moduleKeyName
    }, function(err, data) {
	if (err) {
	    if (err.code === 'InvalidArnException' || err.code === 'NotFoundException') {
		// master key for the module doesn't exist, so
		// create it
		var createKeyParams = {
		    Description : "Lambda Redshift Loader Master Encryption Key",
		    KeyUsage : 'ENCRYPT_DECRYPT'
		};

		// create the master key for this module and
		// bind an alias to it
		kms.createKey(createKeyParams, function(err, createKeyData) {
		    if (err) {
			console.log("Error during Master Key creation");
			return callback(err);
		    } else {
			// create an alias for
			// the master key
			var createAliasParams = {
			    AliasName : moduleKeyName,
			    TargetKeyId : createKeyData.KeyMetadata.KeyId
			};
			kms.createAlias(createAliasParams, function(err, createAliasData) {
			    if (err) {
				console.log("Error during creation of Alias " + moduleKeyName + " for Master Key " + createKeyData.KeyMetadata.Arn);
				return callback(err);
			    } else {
				// invoke
				// the
				// callback
				return callback(undefined, createKeyData.KeyMetadata);
			    }
			});
		    }
		});
	    } else {
		// got an unknown error while describing the key
		console.log("Unknown Error during Customer Master Key describe");
		return callback(err);
	    }
	} else {
	    // ok - we got the previously generated key, so
	    // callback
	    return callback(undefined, data.KeyMetadata);
	}
    });
};
exports.getOrCreateMasterKey = getOrCreateMasterKey;

/**
 * Function which encrypts a value using the module's master key <br>
 * Parameters:
 * <li>toEncrypt - value to be encrypted</li>
 * <li>callback(err, encrypted) - function invoked once encryption is completed</li>
 */
var encrypt = function(toEncrypt, callback) {
    // get the master key
    getOrCreateMasterKey(function(err, keyMetadata) {
	if (err) {
	    console.log("Error during resolution of Customer Master Key");
	    return callback(err);
	} else {
	    // encrypt the data
	    var params = {
		KeyId : keyMetadata.KeyId,
		Plaintext : new Buffer(toEncrypt),
		EncryptionContext : authContext
	    };
	    kms.encrypt(params, function(err, encryptData) {
		if (err) {
		    console.log("Error during Encryption");
		    return callback(err);
		} else {
		    return callback(undefined, encryptData.CiphertextBlob);
		}
	    });
	}
    });
};
exports.encrypt = encrypt;

/**
 * Function which does a blocking encryption of the array of values. Invokes the
 * afterEncryption callback after all values in the input array have been
 * encrypted<br>
 * Parameters:
 * <li>plaintextArray - Array of plaintext input values</li>
 * <li>afterDecryptionCallback - function invoked once encryption has been
 * completed</li>
 */
var encryptAll = function(plaintextArray, afterEncryptionCallback) {
    async.map(plaintextArray, function(item, callback) {
	// decrypt the value using internal decrypt
	encrypt(item, function(err, ciphertext) {
	    return callback(err, ciphertext);
	});
    }, function(err, results) {
	// call the after encryption callback with the result array
	return afterEncryptionCallback(err, results);
    });
};
exports.encryptAll = encryptAll;

var encryptMap = function(valueMap, afterEncryptionCallback) {
    var encryptedValueMap = {};

    async.each(Object.keys(valueMap), function(key, callback) {
	encrypt(valueMap[key], function(err, ciphertext) {
	    if (err) {
		callback(err);
	    } else {
		encryptedValueMap[key] = ciphertext;
		callback();
	    }
	});
    }, function(err) {
	// call the after decryption callback with the result data
	return afterEncryptionCallback(err, encryptedValueMap);
    });
};
exports.encryptMap = encryptMap;

/**
 * Function to decrypt a value using the module's master key<br>
 * Parameters:
 * <li>toDecrypt - value to be decrypted</li>
 * <li>callback(err, decrypted) - Callback to be invoked after decryption which
 * receives the decrypted value, and errors that were generated</li>
 */
var decrypt = function(encryptedCiphertext, callback) {
    var params = {
	CiphertextBlob : encryptedCiphertext,
	EncryptionContext : authContext
    };

    kms.decrypt(params, function(err, decryptData) {
	if (err) {
	    console.log("Error during Decryption");
	    return callback(err);
	} else {
	    if (!decryptData) {
		console.log("Failed to decrypt ciphertext");
		return callback(undefined);
	    } else {
		return callback(undefined, decryptData.Plaintext);
	    }
	}
    });
};
exports.decrypt = decrypt;

/**
 * Function which does a blocking decryption of the array of values. Invokes the
 * afterDecryption callback after all values in the input array have been
 * decrypted<br>
 * Parameters:
 * <li>encryptedArray - Array of encrypted input values</li>
 * <li>afterDecryptionCallback - function invoked once decryption has been
 * completed</li>
 */
var decryptAll = function(encryptedArray, afterDecryptionCallback) {
    async.map(encryptedArray, function(item, callback) {
	// decrypt the value using internal decrypt
	decrypt(item, function(err, plaintext) {
	    return callback(err, plaintext);
	});
    }, function(err, results) {
	// call the after decryption callback with the result array
	return afterDecryptionCallback(err, results);
    });
};
exports.decryptAll = decryptAll;

var decryptMap = function(encryptedValueMap, afterDecryptionCallback) {
    var decryptedValueMap = {};

    async.each(Object.keys(encryptedValueMap), function(key, callback) {
	// decrypt the value using internal decrypt
	decrypt(encryptedValueMap[key], function(err, plaintext) {
	    if (err) {
		console.log(JSON.stringify(err));
		callback(err);
	    } else {
		decryptedValueMap[key] = plaintext;
		callback();
	    }
	});
    }, function(err) {
	// call the after decryption callback with the result data
	return afterDecryptionCallback(err, decryptedValueMap);
    });
};
exports.decryptMap = decryptMap;

var toLambdaStringFormat = function(buffer) {
    return buffer.toString('base64');
};
exports.toLambdaStringFormat = toLambdaStringFormat;
