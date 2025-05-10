import time
import boto3
import sys

class SQSMonitor:
    def __init__(self, queue_name, region="us-east-1"):
        self.queue_name = queue_name
        self.region = region
        self.sqs = boto3.client('sqs', region_name=region)
        self.queue_url = self.sqs.get_queue_url(QueueName=queue_name)['QueueUrl']

    def get_queue_size(self):
        attrs = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        return int(attrs['Attributes'].get('ApproximateNumberOfMessages', 0))

def stream(function_name, maxfunc, queue_name, threshold=10):
    sqs_monitor = SQSMonitor(queue_name)
    lambda_client = boto3.client('lambda', region_name="us-east-1")

    while True:
        queue_size = sqs_monitor.get_queue_size()
        print(f"[INFO] Queue size: {queue_size}")

        workers_needed = min((queue_size // threshold) + 1, maxfunc)

        if queue_size > 0:
            for _ in range(workers_needed):
                print(f"[INFO] Launching Lambda: {function_name}")
                lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType='Event',
                    Payload=b'{}'
                )
        else:
            print("[INFO] Queue empty. No Lambdas launched.")

        time.sleep(5)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Error: function_name, maxfunc, queue_name")
        exit(1)

    stream(sys.argv[1], int(sys.argv[2]), sys.argv[3])