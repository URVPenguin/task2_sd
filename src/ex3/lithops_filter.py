import re
from lithops import FunctionExecutor
from lithops.storage import Storage
from insult_filter import InsultFilter

BUCKET = 'lithops-filter'

def map_func(text_key, storage):
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

def reduce_func(results):
    return sum(results)

if __name__ == '__main__':
    storage = Storage()

    files = [key for key in storage.list_keys(BUCKET) if key.endswith('.txt') and 'censored' not in key]

    print(f"📝 Founded files : {len(files)}")
    if not files:
        print("⚠️ ERROR: No .txt files in the bucket.")
    else:
        with FunctionExecutor() as fexec:
            fexec.map_reduce(map_func, files, reduce_func)
            total = fexec.get_result(show_progressbar=True)
            print(f"✅ Total censored insults: {total}")
