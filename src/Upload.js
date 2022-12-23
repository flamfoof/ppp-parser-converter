import AWS from 'aws-sdk';
import fs from 'fs';
import csvParser from 'csv-parser';
import logbuffer from "console-buffer";

export async function Init() {
    return await UploadToDynamoDB();
}

export async function UploadToDynamoDB() {
    const DynamoDB = new AWS.DynamoDB({ 
        region: process.env.AWS_REGION, 
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY,
            secretAccessKey: process.env.AWS_SECRET_KEY
        }
    });

    var counter = 0;
    console.log("Uploading to DynamoDB")
    
    fs.createReadStream(process.env.input)
    .pipe(csvParser())
    .on('data', function(data) {
        counter++;

        const params = {
            TableName: process.env.DYNAMO_DB_TABLE,
            Item: {
                "LoanNumber": {
                    S: data.LoanNumber
                },
                "DateApproved": {
                    S: data.DateApproved
                },
                "BorrowerName": {
                    S: data.BorrowerName
                },
                "BorrowerAddress": {
                    S: data.BorrowerAddress
                },
                "BorrowerCity": {
                    S: data.BorrowerCity
                },
                "BorrowerState": {
                    S: data.BorrowerState
                },
                "BorrowerZip": {
                    S: data.BorrowerZip
                },
                "LoanStatusDate": {
                    S: data.LoanStatusDate
                },
                "LoanStatus": {
                    S: data.LoanStatus
                },
                "Term": {
                    N: data.Term
                },
                "InitialApprovalAmount": {
                    N: data.InitialApprovalAmount
                },
                "CurrentApprovalAmount": {
                    N: data.CurrentApprovalAmount
                },
                "FranchiseName": {
                    S: data.FranchiseName
                },
                "JobsReported": {
                    S: data.JobsReported
                },
                "NAICSCode": {
                    S: data.NAICSCode
                },
                "Industry": {
                    S: data.Industry
                },
                "BusinessType": {
                    S: data.BusinessType
                },
                "ForgivenessAmount": {
                    S: data.ForgivenessAmount
                },
                "ForgivenessDate": {
                    S: data.ForgivenessDate
                }
            }
        };

        if(!params.LoanNumber)
        {
            console.log("LoanNumber is null");
            console.log(params)
        }
        
        DynamoDB.putItem(params, function(err, data) {
            if (err) 
            {
                console.log(params)
                console.log(err);
                console.log("Stopped at index: " + counter)
                process.exit(1)
            }
        });
        logbuffer.flush();
    })
    .on('end', function() {
        console.log("Completed uploading to DynamoDB");
        console.log("Total Records: " + counter);
    });

    return;
}