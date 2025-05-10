import json
import boto3
import time
from insult_filter import InsultFilter

BUCKET_NAME = "insult-results-bucket"
FILE_KEY = "results.json"

def lambda_handler(event, context):
    insult_filter = InsultFilter()
    #s3 = boto3.client('s3')

    # try:
    #     existing = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    #     results = json.loads(existing['Body'].read().decode('utf-8'))
    # except s3.exceptions.NoSuchKey:
    #     results = []

    results = []

    for record in event['Records']:
        body = json.loads(record['body'])
        text = body.get("text", "")
        filtered = insult_filter.filter_text(text)

        results.append({
            "original": text,
            "filtered": filtered,
            "timestamp": int(time.time())
        })

        # s3.put_object(
        #     Bucket=BUCKET_NAME,
        #     Key=FILE_KEY,
        #     Body=json.dumps(results),
        #     ContentType='application/json'
        # )

    return {"statusCode": 200, "text": json.dumps(results) }