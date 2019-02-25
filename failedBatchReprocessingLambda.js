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
function reprocessEvent(message, callback) {
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

function reprocessMessage(message, callback) {
    // parse out the body of the error from the previous invocation
    if (!message.error) {
        var msg = "Unsupported failure notification structure";
        console.log(JSON.stringify(message));

        callback(msg);
    } else {
        reprocessEvent(message, function (err) {
            callback(err);
        });
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

/**
 * An SQS message subscribed to an SNS notification will take the form:
 *
 {
    "messageId": "62efb910-607f-4f9d-b52e-a8d838ff83f1",
    "receiptHandle": "AQEBdgYzgSq2A+aONxy4lVUqNYx1SQrCXjfVfTeULxiJiLGxAXovwc+obSXzXlcLJ0pxJPCtIinmfdJPGM3KvXEmJZF2RO5vG/N9bKviUR9WuZ4CSokboOZupEyf4PsmqbYS9NZwb4A6YGesKSbhdZdxUCyfPvlP5yaKv9XMbAcJ5nmoIgyOdmCQjzMndj2eLWkzRwxJDdKg9Yusp+O+9GsKpzljYSdSd+ofbTMvv0kWhyoPmGnhVC5TGKXWQXQiei2QLOdasORjUpeF7OPAcZ2Mi8TTergBv6/gMvthRKZajJiSiGgUOebn+RFFXVxeOgg3Gz40DV4SP8iIVQQwWT9Z8v1h7VUubkSGtAvg9j2mIUhQMAhbp99+l0Zv9l1fo2XcI+qsEjYA9ucdIb5ncUE4ke5XFjQgkulZ2niC/0JL+8I=",
    "body": "{\n  \"Type\" : \"Notification\",\n  \"MessageId\" : \"82a21f3c-540a-5d68-8c90-4bd5bd542256\",\n  \"TopicArn\" : \"arn:aws:sns:EXAMPLE\",\n  \"Subject\" : \"Lambda Redshift Batch Load e0d33088-7170-455f-a96c-899d6595dbf3 Failure\",\n  \"Message\" : \"{\\\"error\\\":\\\"{\\\\\\\"redshift-cluster\\\\\\\":{\\\\\\\"status\\\\\\\":-1,\\\\\\\"error\\\\\\\":{\\\\\\\"name\\\\\\\":\\\\\\\"error\\\\\\\",\\\\\\\"length\\\\\\\":169,\\\\\\\"severity\\\\\\\":\\\\\\\"ERROR\\\\\\\",\\\\\\\"code\\\\\\\":\\\\\\\"42703\\\\\\\",\\\\\\\"file\\\\\\\":\\\\\\\"/home/ec2-user/padb/src/pg/src/backend/parser/parse_relation.c\\\\\\\",\\\\\\\"line\\\\\\\":\\\\\\\"2737\\\\\\\",\\\\\\\"routine\\\\\\\":\\\\\\\"attnameAttNum\\\\\\\"}}}\\\",\\\"status\\\":\\\"error\\\",\\\"batchId\\\":\\\"e0d33088-7170-455f-a96c-899d6595dbf3\\\",\\\"s3Prefix\\\":\\\"example-bucket\\\",\\\"key\\\":\\\"lambda-redshift-trigger-file.dummy\\\",\\\"originalManifest\\\":\\\"example-bucket/copy-manifest/manifest-2019-02-22 12:12:12-9042\\\",\\\"failedManifest\\\":\\\"example-bucket/failed-copy-manifest/manifest-2019-02-22 12:12:12-9042\\\"}\",\n  \"Timestamp\" : \"2019-02-22T12:12:14.045Z\",\n  \"SignatureVersion\" : \"1\",\n  \"Signature\" : \"CP5CEpAEUu9J/xwW1ah9UsYK0mFvkv0hcxEXa82Sdtz9s77mWZkSYfI1sNF9RXSozehzmzxNdaXUqafFFzp/0k4esCxQK8vIHuCwrXm2JVrT3qwo3EdkDk6j4IFfR3Q6pt2wnSZkXNGcNuvnK8lbipUfFW703MRttyfuTr7lwh+qcTpc4HoOtv94FdeRgJpL31Oyd1FDT2EX/6Bl2XQkuU6lP78QrQH4XQJonWjSdo/2LsRFrfKIdA0ALdvB/nWHDk748QnLzmkdyg4XIl7a9e06Mz+2o0TvqtRfnPCSQznszBPQ0xLYaxTnxchzSus0qh3TqmWF/5/tEmJZQ5+6Zw==\",\n  \"SigningCertURL\" : \"https://sns.eu-west-1.amazonaws.com/SimpleNotificationService-6aad65c2f9911b05cd53efda11f913f9.pem\",\n  \"UnsubscribeURL\" : \"https://sns.eu-west-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:EXAMPLE:d8aa0a56-e037-448a-ba7f-c570f6f24b4e\"\n}",
    "attributes": {
        "ApproximateReceiveCount": "1",
        "SentTimestamp": "1550837534135",
        "SenderId": "ABC",
        "ApproximateFirstReceiveTimestamp": "1550837534169"
    },
    "messageAttributes": {},
    "md5OfBody": "7ae6d255aa8761d9e1348a9e13585052",
    "eventSource": "aws:sqs",
    "eventSourceARN": "arn:aws:sqs:EXAMPLE",
    "awsRegion": "eu-west-1"
 }
 */
function handleSNS(event, context) {
    if(event.Records[0].eventSource == "aws:sqs" || event.Records[0].EventSource == "aws:sqs") {
        console.log("Received " + event.Records.length + " sqs event(s)");
        for(var i = 0; i < event.Records.length; i++) {
            var body = JSON.parse(event.Records[i].body);
            var message = JSON.parse(body.Message);

            reprocessMessage(message, function(msg) {
                context.done(msg);
            });
        }
        console.log("finished processing sqs messages");
    } else if(event.Records[0].EventSource == "aws:sns") {
        console.log("Received an sns event");
        if (event.Records[0].EventVersion !== "1.0") {
            context.done("Unsupported event version " + event.EventVersion);
        }

        var message = JSON.parse(event.Records[0].Sns.Message);

        reprocessMessage(message, function(msg) {
            context.done(msg);
        });
    } else {
        context.done("Unable to process events of type " + event.Records[0].EventSource);
    }
}

exports.handleSNS = handleSNS;