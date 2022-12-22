import AWS from 'aws-sdk';
import fs from 'fs';
import csvParser from 'csv-parser';

export async function UploadToDynamoDB() {
    const DynamoDB = new AWS.DynamoDB({ region: 'us-east-1' });

    fs.createReadStream('path/to/file.csv')
    .pipe(csvParser())
    .on('data', function(data) {
        const params = {
        TableName: 'my-table',
        Item: {
            'id': { S: data.id },
            'name': { S: data.name },
            'age': { N: data.age }
        }
        };

        DynamoDB.putItem(params, function(err, data) {
        if (err) console.log(err);
        else console.log(data);
        });
    });
}