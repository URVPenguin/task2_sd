from lithops import FunctionExecutor
from lithops.storage import Storage

insults = ['idiota', 'tonto', 'imbécil', 'estúpido', 'inútil', 'burro']
bucket = 'lithops-filter'

def map_function(text_key, storage):
    censored_insults = 0
    obj = storage.get_object(bucket=bucket, key=text_key)
    text = obj.decode('utf-8')

    censored_lines = []
    for line in text.splitlines():
        for insult in insults:
            if insult in line:
                censored_insults += line.count(insult)
                line = line.replace(insult, "CENSORED")
        censored_lines.append(line)

    censored_text = "\n".join(censored_lines)
    censored_key = text_key.replace('.txt', '_censored.txt')
    storage.put_object(bucket=bucket, key=censored_key, body=censored_text.encode('utf-8'))

    return censored_insults

def reduce_function(results):
    return sum(results)

if __name__ == '__main__':
    storage = Storage()

    files = [key for key in storage.list_keys(bucket) if key.endswith('.txt')]

    print(f"📝 Archivos encontrados: {len(files)}")
    if not files:
        print("⚠️ No hay archivos .txt en el bucket.")
    else:
        with FunctionExecutor() as fexec:
            fexec.map_reduce(map_function, files, reduce_function)
            total = fexec.get_result()
            print(f"✅ Total de insultos censurados: {total}")
