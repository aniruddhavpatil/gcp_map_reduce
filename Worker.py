from simple_key_value_store.Client import Client as FS_client
from utils import hash_function
from rpc.Client import Client
import threading
import time
import sys

class Worker:

    def __init__(self, networkConfig,task_id, task_type, n_mappers, n_reducers, files, output_location, function):
        self.networkConfig = networkConfig
        self.task_id = task_id
        self.task_type = task_type
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.files = files
        self.function = function
        self.output_location = output_location
        self.fs_client = FS_client()
        self.fs_client.connect()
        self.rpc = Client(networkConfig)
        self.heartbeat = None
        self.complete = False

    def heartbeat_thread(self):
        while not self.complete:
            self.rpc.run('heartbeat', self.task_id, self.task_type)
            time.sleep(2)

    def start_heartbeat(self):
        self.heartbeat = threading.Thread(target=self.heartbeat_thread)
        self.heartbeat.start()

    def stop_heartbeat(self):
        self.complete = True
        while self.heartbeat.is_alive():
            print("Stopping hearbeat", self.task_id, self.task_type)
            self.heartbeat.join(5)
        return

    def init(self):
        self.start_heartbeat()

    def stop(self):
        self.stop_heartbeat()
        self.rpc.run('signal_complete', self.task_id, self.task_type)


    def run(self):
        # try:
        self.init()
        if(self.task_type == 'map'):
            self.map()

        elif(self.task_type == 'reduce'):
            self.reduce()

        self.stop()
        # except:
        #     print("Something went wrong")
        #     sys.exit()
            # self.rpc.run('fault', self.task_id, self.task_type)

    def emit_intermediate(self, key, value):
        hash_value = hash_function(key, self.n_reducers)
        store_key = 'intermediate_' + str(hash_value)
        store_value = str(key) + ':' + str(value)
        self.fs_client.append(store_key, store_value)

    def emit(self, key, value):
        store_key = self.output_location
        store_value = str(key) + ':' + str(value)
        self.fs_client.append(store_key, store_value)

    def map(self):
        for f in self.files:
            data = self.fs_client.get(f)
            processed_data = self.function(f, data)
            for i, (k,v) in enumerate(processed_data):
                # print(self.task_type + str(self.task_id) + ':' + 'Emitting intermediate data', i+1, 'of', len(processed_data))

                self.emit_intermediate(k,v)
        

    def reduce(self):
        data = self.fs_client.get('intermediate_' + str(self.task_id))
        data = data.split('\n')
        tuple_data = []
        for t in data:
            try:
                k,v = t.split(':')
                tuple_data.append((k,v))
            except ValueError:
                pass
        processed_data = self.function(tuple_data)
        for i, (k, v) in enumerate(processed_data):
            self.emit(k, v)
            # print(self.task_type + str(self.task_id) + ':' + 'Emitting data', i+1, 'of', len(processed_data))

if __name__ == "__main__":
    mapper = Worker('id', 'ip', 'op', 'func')
    mapper.run()