require('./constants');
var batchOperations = require('./batchOperations');

/**
 * Function to act as a filter for error reasons, and whether or not they can be reprocessed
 */
function reprocessSupported(errorReason) {
    // place logic about whether a given error should result in a reprocess
    return true;
}

/** Function that handles an SNS notification of a failed redshift load batch, and calls the reprocessBatch functionality
 *
 * The body of the notification that is sent by the Lambda Loader on failure looks like:
 *
 {
   "batchId": "2790a034-4954-47a9-8c53-624575afd83d",
   "error": "{\"localhost\":{\"status\":-1,\"error\":{\"code\":\"ECONNREFUSED\",\"errno\":\"ECONNREFUSED\",\"syscall\":\"connect\",\"address\":\"127.0.0.1\",\"port\":5439}}}",
   "failedManifest": "meyersi-ire/redshift/manifest/failed/manifest-2018-04-26 10:34:02-5230",
   "key": "input/redshift-input-0.csv",
   "originalManifest": "meyersi-ire/redshift/manifest/manifest-2018-04-26 10:34:02-5230",
   "s3Prefix": "lambda-redshift-loader-test/input",
   "status": "error"
 }
 *
 */
function reprocessEvent(message, messageAttributes, callback) {
    // unwrap the structure of the error body - it's currently indexed by the ID of the failing redshift cluster
    var failureEventError = JSON.parse(message.error);
    var errorBody = failureEventError[Object.keys(failureEventError)[0]].error;

    // hand over to the function that makes the go/no-go decision on processing failure events
    if (reprocessSupported(errorBody.code)) {
        // call the reprocess batch API of batchOperations, which will resubmit the processed files for processing
        batchOperations.reprocessBatch(message.s3Prefix, message.batchId, process.env['AWS_REGION'], undefined, function (err) {
            if (err) {
                callback(err);
            } else {
                console.log("Failure Notification processing complete. Batch " + message.batchId + " submitted for reprocessing");
                callback();
            }
        });
    } else {
        console.log("Failure Event Code " + failureEventError.error.code + " not supported for Batch resubmission. No action taken");
        callback();
    }
}

/**
 * An SNS notification will take the form:
 *
 {
   "Records": [
     {
       "EventVersion": "1.0",
       "EventSubscriptionArn": "arn:aws:sns:EXAMPLE",
       "EventSource": "aws:sns",
       "Sns": {
         "SignatureVersion": "1",
         "Timestamp": "1970-01-01T00:00:00.000Z",
         "Signature": "EXAMPLE",
         "SigningCertUrl": "EXAMPLE",
         "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
         "Message": "Hello from SNS!",
         "MessageAttributes": {
           "Test": {
             "Type": "String",
             "Value": "TestString"
           },
           "TestBinary": {
             "Type": "Binary",
             "Value": "TestBinary"
           }
         },
         "Type": "Notification",
         "UnsubscribeUrl": "EXAMPLE",
         "TopicArn": "arn:aws:sns:EXAMPLE",
         "Subject": "TestInvoke"
       }
     }
   ]
 }
 */
function handleSNS(event, context) {
    // basic filtering of events that we can support
    if (event.Records[0].EventSource !== "aws:sns") {
        context.done("Unable to process events of type " + event.EventSource);
    }

    if (event.Records[0].EventVersion !== "1.0") {
        context.done("Unsupported event version " + event.EventVersion);
    }

    var message = event.Records[0].Sns.Message;

    // parse out the body of the error from the previous invocation
    if (!message.error) {
        var msg = "Unsupported failure notification structure";
        console.log(JSON.stringify(message));
        console.log(msg);
        context.done(msg);
    } else {
        reprocessEvent(message, event.Records[0].Sns.MessageAttributes, function (err) {
            context.done(err);
        });
    }
}

exports.handleSNS = handleSNS;