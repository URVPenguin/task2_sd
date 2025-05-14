import math
import re
from lithops import FunctionExecutor
from lithops.storage import Storage
from insult_filter import InsultFilter


def map_func(text_key, storage):
    BUCKET = 'lithops-filter'
    obj = storage.get_object(bucket=BUCKET, key=text_key)
    text = obj.decode('utf-8')
    insult_filter = InsultFilter()

    censored_insults = 0
    for insult in sorted(insult_filter.insults, key=len, reverse=True):
        matches = re.findall(rf'\b{re.escape(insult)}\b', text, flags=re.IGNORECASE)
        censored_insults += len(matches)
        text = re.sub(rf'\b{re.escape(insult)}\b', "CENSORED", text, flags=re.IGNORECASE)

    censored_key = text_key.replace('.txt', '_censored.txt')
    storage.put_object(bucket=BUCKET, key=censored_key, body=text.encode('utf-8'))

    return censored_insults


def run_batch(map_function, maxfunc, bucket):
    storage = Storage()
    file_list = storage.list_keys(bucket)

    print(f"📝 Founded files : {len(file_list)}")
    if not file_list:
        print("⚠️ ERROR: No .txt files in the bucket.")
        exit(1)

    n_batches = math.ceil(len(file_list) / maxfunc)
    print(f"Processing {len(file_list)} files in {n_batches} batches of maxfunc = {maxfunc}")

    results = []
    for i in range(0, len(file_list), maxfunc):
        batch = file_list[i:i + maxfunc]
        with FunctionExecutor() as fexec:
            fexec.map(map_function, batch)
            results += fexec.get_result()
        print(f"✅ Results: {results}")

    print(f"✅ Final Results: {results}")
    return results


if __name__ == '__main__':
    print(run_batch(map_func, 2, 'lithops-filter'))