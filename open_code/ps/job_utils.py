def normalize_storage_size(size):
    import re
    if not isinstance(size, str) or not re.match(r'\d+[MG]', size):
        message = "'size' must be a string like 4096M or 4G; "
        message += "%r is invalid" % size
        raise ValueError(message)
    value = int(size[:-1])
    unit = size[-1]
    if unit == 'G':
        value *= 1024
    return value

def merge_storage_size(worker_memory, server_memory):
    mem1 = normalize_storage_size(worker_memory)
    mem2 = normalize_storage_size(server_memory)
    mem = max(mem1, mem2)
    if mem % 1024 == 0:
        mem = '%dG' % (mem // 1024)
    else:
        mem = '%dM' % mem
    return mem
