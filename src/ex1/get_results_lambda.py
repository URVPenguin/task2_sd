import json
import boto3

BUCKET_NAME = "insult-results-bucket"
FILE_KEY = "results.json"

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    try:
        data = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
        results = json.loads(data['Body'].read().decode('utf-8'))
    except s3.exceptions.NoSuchKey:
        results = []

    results.sort(key=lambda x: x['timestamp'], reverse=True)

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }