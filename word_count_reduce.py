def reduce_fn(tuple_data):
    D = {}
    for k, v in tuple_data:
        if k not in D:
            D[k] = 1
        else:
            D[k] += 1
    return tuple(list(zip(D.keys(), D.values())))
