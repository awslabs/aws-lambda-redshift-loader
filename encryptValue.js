/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */
require('./constants');
var kmsCrypto = require('./kmsCrypto');
var args = require('minimist')(process.argv.slice(2));
var region = args.region;
var input = args.input;
var debug = (process.env['DEBUG'] === 'true');
var log_level = process.env['LOG_LEVEL'] || 'info';
const winston = require('winston');

const logger = winston.createLogger({
	level : debug === true ? 'debug' : log_level,
	transports : [ new winston.transports.Console({
		format : winston.format.simple()
	}) ]
});

function encrypt(region, input, callback) {
	kmsCrypto.setRegion(region);

	kmsCrypto.encrypt(input, function(err, encryptedCiphertext) {
		if (err)
			return callback(err);

		kmsCrypto.decrypt(encryptedCiphertext, function(err, plaintext) {
			if (err)
				return callback(err);

			if (plaintext.toString() === input) {
				logger.info("Encryption completed and verified with AWS KMS");

				callback({
					inputValue : input,
					configurationEntryValue : kmsCrypto.toLambdaStringFormat(encryptedCiphertext)
				});
			} else {
				callback("Encryption completed but could not be validated. Result: " + plaintext.toString());
			}
		});

	});
}
exports.encrypt = encrypt;

if (!region || !input) {
	logger.error("You must provide a region (--region) for the KMS Service and an input value (--input) to Encrypt");
	process.exit(ERROR);
} else {
	encrypt(region, input, function(err, result) {
		if (err) {
			logger.error(err);
			process.exit(ERROR);
		} else {
			logger.info(JSON.stringify(result));
		}
	});
}