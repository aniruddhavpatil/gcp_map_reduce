import json
import importlib

class Config:
    def __init__(self, file, mode="default"):
        self.file = open(file, 'r')
        self.config = json.loads(self.file.read())
        self.config = self.config[mode]

    def get_network_config(self,config):
        address, port = config
        return (address, port)

    def get_map_method(self, location):
        module = importlib.import_module(location)
        method = getattr(module, 'map_fn')
        return method


    def get_reduce_method(self, location):
        module = importlib.import_module(location)
        method = getattr(module, 'reduce_fn')
        return method

    def parse(self):
        self.project = self.config['project']
        self.zone = self.config['zone']
        self.network_config = self.get_network_config(self.config['network_config'])
        self.input_data = self.config['input_data']
        self.mapper_count = self.config['n_mappers']
        self.reducer_count = self.config['n_reducers']
        self.output_data = self.config['output_data']
        self.map_fn = self.get_map_method(self.config['map_fn'])
        self.reduce_fn = self.get_reduce_method(self.config['reduce_fn'])
