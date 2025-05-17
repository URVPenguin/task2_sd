import multiprocessing
import time

import boto3
import json

def submit_text(_):
    sqs = boto3.client('sqs')
    return sqs.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/253143858076/submit_text_queue',
        MessageBody=json.dumps({"text": 'You are a fucking idiot, stupid bitch'})
    )

if __name__ == "__main__":
    for _ in range(3):
        with multiprocessing.Pool(processes=100) as pool:
            results = pool.map(submit_text, range(1000))
        print(results)
        time.sleep(10)

    for _ in range(10):
        with multiprocessing.Pool(processes=5) as pool:
            results = pool.map(submit_text, range(10))
        print(results)
        time.sleep(5)


