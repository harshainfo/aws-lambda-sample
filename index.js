const { DynamoDBClient, PutItemCommand } = require("@aws-sdk/client-dynamodb");
const { S3Client, HeadObjectCommand } = require("@aws-sdk/client-s3");

// Set the AWS region
const region = 'us-east-1';

// Create DynamoDB and S3 clients
const dynamodbClient = new DynamoDBClient({ region });
const s3Client = new S3Client({ region });

// Define the DynamoDB table name
const tableName = process.env.tableName;

exports.handler = async (event) => {
  try {
    // Get the S3 object details from the event
    const { bucket, object } = event.Records[0].s3;

    // Get the object metadata
    const objectMetadata = await s3Client.send(
      new HeadObjectCommand({
        Bucket: bucket.name,
        Key: object.key,
      })
    );

    // Prepare the DynamoDB item
    const item = {
      id: { S: object.key },
      createdAt: { S: objectMetadata.LastModified.toISOString() },
      size: { N: objectMetadata.ContentLength.toString() },
      contentType: { S: objectMetadata.ContentType },
      // Add any other relevant metadata fields
    };

    // Write the item to the DynamoDB table
    await dynamodbClient.send(
      new PutItemCommand({
        TableName: tableName,
        Item: item,
      })
    );

    console.log(`Successfully processed ${object.key} and wrote to DynamoDB.`);
    return { statusCode: 200, body: 'Success' };
  } catch (err) {
    console.error(`Error processing ${event.Records[0].s3.object.key}`, err);
    throw err;
  }
};