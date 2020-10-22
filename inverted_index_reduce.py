def reduce_fn(tuple_data):
    D = {}
    for k, v in tuple_data:
        if k not in D:
            D[k] = {}
            D[k][v] = 1
        else:
            if v not in D[k]:
                D[k][v] = 1
            else:
                D[k][v] += 1
    for k in D:
        curr_list = list(zip(D[k].keys(), D[k].values()))
        curr_list.sort(key=lambda x: x[1], reverse=True)
        D[k] = curr_list

    ret = []
    for item in list(zip(D.keys(), D.values())):
        value_string = ''
        for value in item[1]:
            value_string += value[0] + ' ' + str(value[1]) + ','
        ret.append((item[0], value_string[:-1]))
    return ret
