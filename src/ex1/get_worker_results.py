import boto3
import json

BUCKET_NAME = "insult-results-bucket"

def lambda_handler(event, context):
    s3 = boto3.client('s3', region_name='us-east-1')

    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)

        results = []
        for obj in response.get('Contents', []):
            content = s3.get_object(Bucket=BUCKET_NAME, Key=obj['Key'])
            file_content = content['Body'].read().decode('utf-8')
            results.extend(file_content.split("\n"))

        return {
            "StatusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(results)
        }
    except Exception as e:
        return {
            "StatusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
