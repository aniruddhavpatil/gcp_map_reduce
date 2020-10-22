def map_fn(key, value):
    ret = []
    for w in value.split():
        ret.append((w.strip(':'), key))
    return ret
