import multiprocessing

import boto3
import json

def submit_text():
    sqs = boto3.client('sqs', region_name='us-east-1')
    return sqs.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/568794428298/submit_text_queue',
        MessageBody=json.dumps({"text": 'You are a fucking idiot'})
    )

if __name__ == "__main__":
    with multiprocessing.Pool(processes=200) as pool:
        results = pool.map(submit_text, [])

    for result in results:
        print(result)
