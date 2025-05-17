import boto3
import json

DYNAMODB_TABLE_NAME = "InsultResultsTable"


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    try:
        response = table.scan(
            Limit=100,
            Select='ALL_ATTRIBUTES'
        )

        items = sorted(response['Items'], key=lambda x: x['timestamp'], reverse=True)

        last_100_items = items[:100]

        results = [item['filtered_text'] for item in last_100_items]

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