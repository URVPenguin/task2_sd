import math
from lithops import FunctionExecutor
from lithops.storage.utils import StorageNoSuchKeyError


def run_batch(map_function, maxfunc, bucket, config):
    with FunctionExecutor(config=config) as fexec:
        storage = fexec.storage
        file_list = storage.list_keys(bucket, prefix='insultos/')

    total = 0
    n_batches = math.ceil(len(file_list) / maxfunc)

    print(f"Procesando {len(file_list)} archivos en {n_batches} lotes de {maxfunc} funciones como máximo")

    for i in range(0, len(file_list), maxfunc):
        batch = file_list[i:i + maxfunc]
        with FunctionExecutor(config=config) as fexec:
            fexec.map(map_function, batch)
            results = fexec.get_result()
            total += sum(results)

    print(f"✅ Total de insultos censurados: {total}")
    return total
