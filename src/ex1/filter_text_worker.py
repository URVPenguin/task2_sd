import json
import boto3
import time
import uuid
from insult_filter import InsultFilter

BUCKET_NAME = "insult-results-bucket"

def lambda_handler(event, context):
    insult_filter = InsultFilter()
    s3 = boto3.client('s3', region_name='us-east-1')

    results = []
    for record in event['Records']:
        body = json.loads(record['body'])
        text = body.get("text", "")
        filtered = insult_filter.filter_text(text)
        results.append(filtered)

    file_id = str(uuid.uuid4())
    timestamp = int(time.time())
    key = f"filtered_insults_{timestamp}_{file_id}.txt"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body="\n".join(results),
        ContentType='text/plain'
    )

    return {
        "StatusCode": 200,
        "body": json.dumps({
            "message": f"Saved {len(results)} results to {key}",
            "s3_key": key
        })
    }
