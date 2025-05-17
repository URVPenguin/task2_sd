import json
import boto3
import time
import uuid
from insult_filter import InsultFilter

DYNAMODB_TABLE_NAME = "InsultResultsTable"


def lambda_handler(event, context):
    insult_filter = InsultFilter()
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)

        for record in event['Records']:
            body = json.loads(record['body'])
            text = body.get("text", "")
            filtered = insult_filter.filter_text(text)

            item_id = str(uuid.uuid4())
            timestamp = int(time.time())

            table.put_item(
                Item={
                    'id': item_id,
                    'timestamp': timestamp,
                    'original_text': text,
                    'filtered_text': filtered,
                }
            )

        return {
            "StatusCode": 200,
            "body": json.dumps({
                "message": f"Processed {len(event['Records'])} records and saved to DynamoDB",
                "table_name": DYNAMODB_TABLE_NAME
            })
        }
    except Exception as e:
        return {
            "StatusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }

