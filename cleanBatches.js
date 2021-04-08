var batchOperations = require("./batchOperations");

var args = require('minimist')(process.argv.slice(2));

var setRegion = args.region;
var s3Prefix = args.s3Prefix;

batchOperations.cleanBatches(setRegion, s3Prefix, function (err, data) {
    if (err) {
        console.log("Error: " + err);
        process.exit(-1);
    } else {
        console.log("OK: Deletion of " + data.batchCountDeleted + " Batches");
        console.log("Deleted Batch Information:");
        console.log(JSON.stringify(data));

    }
})
