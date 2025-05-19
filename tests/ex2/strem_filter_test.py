import multiprocessing
from time import sleep

import boto3
import json

def submit_text(_):
    sqs = boto3.client('sqs')
    return sqs.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/260339617870/stream_submit_text_queue',
        MessageBody=json.dumps({"text": 'You are a fucking idiot, stupid bitch'})
    )

if __name__ == "__main__":
    requests = [5, 5, 200, 500, 1000, 200, 50, 30, 50, 20, 10, 5, 5, 5, 5, 500]
    for req in requests:
        print("Sending request: ", req)
        with multiprocessing.Pool(processes=10) as pool:
            pool.map(submit_text, range(req))
        sleep(7)