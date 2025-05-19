import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
import boto3
import sys

class SQSQueue:
    def __init__(self, queue_name, region="us-east-1"):
        self.queue_name = queue_name
        self.region = region
        self.sqs = boto3.client('sqs', region_name=region)
        self.queue_url = self.sqs.get_queue_url(QueueName=queue_name)['QueueUrl']

    def get_queue_statistics(self):
        attrs = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible'
            ]
        )
        return int(attrs['Attributes'].get('ApproximateNumberOfMessages', 0)), int(attrs['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))

    def get_messages(self, max_messages=10):
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages
        )
        return response.get('Messages', [])

    def delete_message(self, receipt_handle):
        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )


def invoke_lambda(lambda_client, function_name, messages, sqs_queue):
    records = []
    for msg in messages:
        records.append({ "body": msg["Body"] })

    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({ "Records": records }).encode('utf-8')
        )

        response_payload = json.loads(response['Payload'].read())

        if response.get("StatusCode") == 200:
            for msg in messages:
                sqs_queue.delete_message(msg['ReceiptHandle'])
                #print(f"[SUCCESS] Deleted message: {msg['MessageId']}")
        else:
            print(f"[FAIL] Lambda failed {response_payload}")
    except Exception as e:
        print(f"[ERROR] Lambda invocation failed: {e}")

def stream(function_name, maxfunc, queue_name):
    sqs_queue = SQSQueue(queue_name)
    lambda_client = boto3.client('lambda', region_name="us-east-1")
    lambda_batch_size = 10

    with ThreadPoolExecutor(max_workers=maxfunc) as executor:
        futures = set()

        while True:
            done = {f for f in futures if f.done()}
            futures -= done

            queue_size, messages_processing = sqs_queue.get_queue_statistics()
            required_invocations = math.ceil(queue_size // lambda_batch_size)
            if required_invocations == 0:
                required_invocations = queue_size % lambda_batch_size
            available_slots = maxfunc - len(futures)

            if queue_size <= 0 or available_slots <= 0 or min(available_slots, required_invocations) == 0:
                time.sleep(0.2)
                continue

            for invoke in range(min(available_slots, required_invocations)):
                messages = sqs_queue.get_messages()
                if not messages:
                    continue
                future = executor.submit(invoke_lambda, lambda_client, function_name, messages, sqs_queue)
                futures.add(future)

            time.sleep(0.1)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Error: function_name, maxfunc, queue_name")
        exit(1)

    stream(sys.argv[1], int(sys.argv[2]), sys.argv[3])