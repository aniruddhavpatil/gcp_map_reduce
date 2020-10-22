def map_fn(key, value):
    ret = []
    for w in value.split():
        ret.append((w.strip(':'), 1))
    return ret
