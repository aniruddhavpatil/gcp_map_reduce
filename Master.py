from rpc.Server import Server
import multiprocessing as mp
import time
import os
from simple_key_value_store.Client import Client as FS_client
from Worker import Worker
import importlib
from utils import clear_store
import threading
import sys

from GCP import CloudInterface
from Configuration import Config

class ProcessFactoryElement:
    def __init__(self, process_id, process):
        self.timestamp = time.time()
        self.process = process
        self.process_id = process_id
        self.max_wait_time = 5
        self.status = "initialized"

    def is_complete(self):
        return self.status == "completed"

    def start(self):
        self.process.start()
        self.status = "running"
    
    def stop(self):
        self.status = "completed"
        self.process.join(1)
        while self.process.is_alive():
            self.process.terminate()
            self.process.close()


class ProcessFactory:
    def __init__(self):
        self.processes = {}

    def get_pid(self, task_type, task_id):
        pid = str(task_type) + str(task_id)
        return pid

    def create_process(self, task_type, task_id, worker):
        pid = self.get_pid(task_type, task_id)
        self.processes[pid] = ProcessFactoryElement(pid, mp.Process(target=worker.run))
    
    def start_process(self, task_type, task_id):
        pid = self.get_pid(task_type, task_id)
        self.processes[pid].start()

    def end_process(self, task_type, task_id):
        pid = self.get_pid(task_type, task_id)
        t = threading.Thread(target=self.processes[pid].stop())
        t.start()

    def check_process_completion_by_type(self, task_type):
        if len(self.processes) > 0:
            for key in self.processes:
                if task_type in key:
                    if not self.processes[key].is_complete():
                        return False
        else:
            print('No process has been started as of now.')
        return True

    def print_status(self):
        for key in self.processes:
            process = self.processes[key]
            print(key, process.status)

class Master:
    def __init__(self, gcp, networkConfig, methods, n_mappers, n_reducers, map_fn, reduce_fn, input_data, output_data):
        self.gcp = gcp
        self.server = Server(networkConfig, [self.signal_complete, self.heartbeat, self.fault, *methods])
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers

        self.input_data = input_data
        self.output_data = output_data

        self.map_fn = map_fn
        self.reduce_fn = reduce_fn
        self.networkConfig = networkConfig
        self.base_dir = os.getcwd()
        self.fs_client = None
        self.file_dict = {}
        self.mappers = {}
        self.reducers = {}
        self.splits = {}
        self.factory = ProcessFactory()

    def heartbeat(self, task_id, task_type):
        print('Heartbeat from', task_id, task_type)


    def set_attribute(self, attribute, value):
        attribute = self.__getattribute__(attribute)
        attribute = value
        return True


    def input_partition(self, files, n):
        for f in files:
            print('Fetching data from', f)
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

    def signal_complete(self, task_id, task_type):
        print("Signal complete", task_id, task_type)
        self.factory.end_process(task_type, task_id)
        

    def create_worker(self, worker, target):
        return {
            "worker": mp.Process(target=target),
            "timestamp": time.time(),
            "status": "running",
        }

    def fault(self, task_id, task_type):
        if task_type == 'map':
            self.restart_map(task_id)
        elif task_type == 'reduce':
            self.restart_reducers()

    def get_pid(self, task_type, task_id):
        pid = str(task_type) + str(task_id)
        return pid

    def start_workers(self, task_type):
        if task_type == 'map':
            passed_function = self.map_fn
            count = self.n_mappers
        elif task_type == 'reduce':
            passed_function = self.reduce_fn
            count = self.n_reducers
        print('Starting workers of type', task_type)

        if passed_function and count:
            for task_id in range(count):
                # files = self.splits[task_id] if task_type == 'map' else None
                pid = self.get_pid(task_type, task_id)
                operation = self.gcp.create_instance(pid, init_script="worker.sh", preemptible=True)
                self.gcp.wait_for_operation(operation['name'])


    def restart_map(self, task_id):
        self.fs_client.set(self.splits[task_id], '')
        worker = Worker(self.networkConfig, task_id, 'map',
                        self.n_mappers, self.splits[task_id], self.output_data, self.map_fn)
        recovery = self.create_worker(worker, worker.run)
        self.mappers[task_id] = recovery
        recovery.start()
    
    def init_fs_client(self):
        fs_client_ip = self.gcp.get_ip_from_name('store', True)
        print('Store found at', fs_client_ip)
        self.fs_client = FS_client((fs_client_ip, 80))
        self.fs_client.connect()
        print(self.fs_client.get('key24'))
        # print(self.fs_client.set('key24', 'adsfasdfds'))
        # print(self.fs_client.get('key24'))


    def run(self):
        try:
            self.init_fs_client()
            self.server_process = threading.Thread(target=self.server.run)
            print('Starting Master')
            self.server_process.start()
            self.splits = self.input_partition([self.input_data], self.n_mappers)
            ## TODO: cloud worker factory
            self.start_workers('map')
            self.gcp.update_instances()
            for key in self.gcp.instances:
                val = self.gcp.instances[key]
                print('Running:', key, self.gcp.get_ip_from_name(key))
            # time.sleep(60)
            self.stop_instances()
            self.start_workers('reduce')
            time.sleep(30)
            self.gcp.update_instances()
            print(list(self.gcp.instances.keys()))
            for key in self.gcp.instances:
                val = self.gcp.instances[key]
                print('Running:', key, self.gcp.get_ip_from_name(key))
            # time.sleep(60)
            self.stop_instances()
            
        except KeyboardInterrupt:
            self.stop()
        self.stop()

    def stop_process(self, process):
        process.join(1)
        while process.is_alive():
            process.terminate()
            process.close()


    def stop_mappers(self):
        for key in self.mappers:
            p = self.mappers[key]["worker"]
            self.stop_process(p)
        self.mappers = {}

    def stop_reducers(self):
        for key in self.reducers:
            p = self.reducers[key]["worker"]
            self.stop_process(p)
        self.reducers = {}

    def stop(self):
        print("Stopping mappers")
        self.stop_mappers()
        print("Stopping reducers")
        self.stop_reducers()
        
        print("Stopping RPC sercer")
        try:
            self.server.kill()
            self.stop_process(self.server_process)
        except:
            pass
        print('Stopping Master')
        sys.exit()

    def stop_instances(self):
        self.gcp.update_instances()
        for key in self.gcp.instances:
            if 'map' in key or 'reduce' in key:
                operation = self.gcp.delete_instance(key)
                self.gcp.wait_for_operation(operation['name'])

def myFunc(i):
    print(i)

def run_local():
    module = importlib.import_module('word_count_map')
    map_fn = getattr(module, 'map_fn')
    module = importlib.import_module('word_count_reduce')
    reduce_fn = getattr(module, 'reduce_fn')
    master = Master(('', 80), [myFunc], 4, 4, map_fn, reduce_fn)
    # master.run()

def run_cloud():
    cfg = Config('config.json')
    cfg.parse()
    gcp = CloudInterface(cfg.project, cfg.zone)
    master = Master(
        gcp,
        cfg.network_config,
        [],
        cfg.mapper_count,
        cfg.reducer_count,
        cfg.map_fn,
        cfg.reduce_fn,
        cfg.input_data,
        cfg.output_data
    )
    master.run()
    

if __name__ == "__main__":
    run_cloud()

