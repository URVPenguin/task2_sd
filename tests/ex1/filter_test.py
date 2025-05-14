import multiprocessing
import boto3
import json

def submit_text(_):
    sqs = boto3.client('sqs')
    return sqs.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/568794428298/submit_text_queue',
        MessageBody=json.dumps({"text": 'You are a fucking idiot, stupid bitch'})
    )

if __name__ == "__main__":
    with multiprocessing.Pool(processes=50) as pool:
        results = pool.map(submit_text, range(50))

    for result in results:
        print(result)
