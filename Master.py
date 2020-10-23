from rpc.Server import Server
import multiprocessing as mp
import time
import os
from simple_key_value_store.Client import Client as FS_client
from Worker import Worker
import importlib
from utils import clear_store


class Master:
    def __init__(self, networkConfig, methods, n_mappers, n_reducers, map_fn, reduce_fn, input_data, output_data):
        self.server = Server(networkConfig, [self.fault, *methods])
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers

        self.input_data = input_data
        self.output_data = output_data

        self.map_fn = map_fn
        self.reduce_fn = reduce_fn
        self.networkConfig = networkConfig
        self.base_dir = os.getcwd()
        self.fs_client = FS_client()
        self.fs_client.connect()
        self.file_dict = {}
        self.mappers = []
        self.reducers = []
        self.splits = {}

    def set_attribute(self, attribute, value):
        attribute = self.__getattribute__(attribute)
        attribute = value
        return True


    def input_partition(self, files, n):
        for f in files:
            data = self.fs_client.get(f)
            total_size = len(data)
            split_size = total_size // n
            chunked_size = 0
            chunk = 0
            while chunk < n:
                key = chunk
                chunk_name = 'split_' + str(key) + '_' + f
                if chunk < n - 1:
                    chunked_size = (chunk + 1) * split_size
                    self.fs_client.set(chunk_name,data[chunk * split_size :  chunked_size])
                else:
                    self.fs_client.set(chunk_name, data[chunk * split_size: ])
                chunk+=1
                if not key in self.file_dict:
                    self.file_dict[key] = [chunk_name]
                else:
                    self.file_list[key].append(chunk_name)
        return self.file_dict

    def restart_reducers(self):
        self.stop_reducers()
        self.fs_client.set(self.output_data, '')
        self.start_reducers()

    def fault(self, task_id, task_type):
        if task_type == 'map':
            self.restart_map(task_id)
        elif task_type == 'reduce':
            self.restart_reducers()

    def start_mappers(self):
        print('Starting mappers')
        for i in range(self.n_mappers):
            worker = Worker(self.networkConfig, i, 'map', self.n_mappers,
                            self.n_reducers, self.splits[i], self.output_data, self.map_fn)
            self.mappers.append(mp.Process(target=worker.run))

        for i, w in enumerate(self.mappers):
            print('Mapper:', i, 'started')
            w.start()

        for i, w in enumerate(self.mappers):
            print('Mapper:', i, 'completed')
            w.join()

    def start_reducers(self):
        print('Starting reducers')
        for i in range(self.n_reducers):
            worker = Worker(self.networkConfig, i, 'reduce',
                            self.n_mappers, self.n_reducers, None, self.output_data, self.reduce_fn)
            self.reducers.append(mp.Process(target=worker.run))

        for i, w in enumerate(self.reducers):
            print('Reducer:', i, 'started')
            w.start()

        for i, w in enumerate(self.reducers):
            print('Reducer:', i, 'completed')
            w.join()

    def restart_map(self, task_id):
        self.fs_client.set(self.splits[task_id], '')
        worker = Worker(self.networkConfig, task_id, 'map',
                        self.n_mappers, self.splits[task_id], self.output_data, self.map_fn)
        recovery = mp.Process(target=worker.run)
        recovery.start()
        recovery.join()
        

    def run(self):
        try:
            self.server_process = mp.Process(target=self.server.run)
            print('Starting Master')
            self.server_process.start()
            self.splits = self.input_partition([self.input_data], self.n_mappers)
            self.start_mappers()
            self.start_reducers()

        except KeyboardInterrupt:
            self.stop()
        self.stop()


    def stop_mappers(self):
        for i, p in enumerate(self.mappers):
            try:
                p.terminate()
                p.join()
                p.close()
            except:
                pass

    def stop_reducers(self):
        for i, p in enumerate(self.reducers):
            try:
                p.terminate()
                p.join()
                p.close()
            except:
                pass

    def stop(self):
        print('Stopping Master')

        self.stop_mappers()
        self.stop_reducers()
        
        try:
            self.server.kill()
            self.server_process.terminate()
            self.server_process.join()
            self.server_process.close()
        except:
            pass

def myFunc(i):
    print(i)

def main():
    module = importlib.import_module('word_count_map')
    map_fn = getattr(module, 'map_fn')
    module = importlib.import_module('word_count_reduce')
    reduce_fn = getattr(module, 'reduce_fn')
    master = Master(('localhost', 8000), [myFunc], 4, 4, map_fn, reduce_fn)
    # master.run()

if __name__ == "__main__":
    main()
