var readline = require('readline');
var aws = require('aws-sdk');
var dynamoDB;
require('../../constants');
var setRegion = 'us-east-1';
var common = require('../../common');

var rl = readline.createInterface({
	input : process.stdin,
	output : process.stdout
});

rl.question('Enter the Region for the Redshift Load Configuration > ', function(answer) {
	if (common.blank(answer) !== null) {
		setRegion = answer;
	} else {
		console.log('Using the default region ' + setRegion);
	}

	rl.close();

	dynamoDB = new aws.DynamoDB({
		apiVersion : '2012-08-10',
		region : setRegion
	});
	
	common.dropTables(dynamoDB);
});
