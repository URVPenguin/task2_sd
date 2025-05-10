import json
import boto3
from insult_filter import InsultFilter
import time

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/568794428298/stream_submit_text_queue'

def lambda_handler(event, context):
    sqs = boto3.client('sqs', region_name='us-east-1')
    messages = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=10
    ).get('Messages', [])

    insult_filter = InsultFilter()
    results = []

    for msg in messages:
        body = json.loads(msg['Body'])
        text = body.get("text", "")
        filtered = insult_filter.filter_text(text)

        results.append({
            "original": text,
            "filtered": filtered,
            "timestamp": int(time.time())
        })

        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=msg['ReceiptHandle']
        )

    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }
