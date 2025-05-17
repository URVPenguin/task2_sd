import multiprocessing
from time import sleep

import boto3
import json

def submit_text(_):
    sqs = boto3.client('sqs')
    return sqs.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/568794428298/stream_submit_text_queue',
        MessageBody=json.dumps({"text": 'You are a fucking idiot, stupid bitch'})
    )

if __name__ == "__main__":
    requests = [30, 10, 20, 30, 60, 70, 80, 200, 300, 400, 500, 10, 5, 5, 5, 5]
    for req in requests:
        with multiprocessing.Pool(processes=10) as pool:
            pool.map(submit_text, range(req))
        sleep(5)