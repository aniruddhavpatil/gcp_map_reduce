import json
from Master import Master
import importlib
import sys


def get_network_config(config):
    address, port = config
    return (address, port)

def get_map_method(location):
    module = importlib.import_module(location)
    method = getattr(module, 'map_fn')
    return method


def get_reduce_method(location):
    module = importlib.import_module(location)
    method = getattr(module, 'reduce_fn')
    return method

if __name__ == '__main__':
    

    if len(sys.argv) != 2:
        print("ERROR: Expected 1 argument")
        sys.exit()

    mode = sys.argv[1]
    permitted_modes = ['word_count', 'inverted_index']
    if mode not in permitted_modes:
        print("ERROR: Given mode not supported")
        print("Permitted Modes:")
        print(permitted_modes)
        sys.exit()

    f = open('config.json', 'r')
    config = json.loads(f.read())
    
    config = config[mode]
    network_config = get_network_config(config['network_config'])
    input_data = config['input_data']
    mapper_count = config['n_mappers']
    reducer_count = config['n_reducers']
    output_data = config['output_data']
    map_fn = get_map_method(config['map_fn'])
    reduce_fn = get_reduce_method(config['reduce_fn'])

    master = Master(network_config, [], mapper_count, reducer_count, map_fn, reduce_fn, input_data, output_data)
    master.run()
