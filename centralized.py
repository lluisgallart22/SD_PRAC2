import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + './proto')
sys.path.append(os.getcwd())

import grpc
import store_pb2
import store_pb2_grpc
import threading
import yaml
from concurrent.futures import ThreadPoolExecutor
import time

config_path = os.path.join(os.path.dirname(__file__), 'configuration', 'centralized_config.yaml')
#Ports dels nodes i ip
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

master_ip = config['master']['ip']
master_port = config['master']['port']

slaves = config.get('slaves', [])
slaves_info = [(slave['ip'], slave['port']) for slave in slaves]

#Implementació del KeyValueStore
class KeyValueStoreService(store_pb2_grpc.KeyValueStoreServicer):

    def __init__(self, node_id):
        self.store = {}
        self.delay = 0
        self.node_id = node_id
        self.lock = threading.Lock()
        self._load_data_from_yaml()

    def put(self, request, context):
        key = request.key
        value = request.value
        if self.canCommit(request, context):
            with self.lock:
                self.doCommit(request, context)
            self._apply_delay()
            return store_pb2.PutResponse(success=True)
        else:
            self.doAbort(request, context)
            self._apply_delay()
            return store_pb2.PutResponse(success=False)
        
    def get(self, request, context):
        key = request.key
        if key in self.store:
            self._apply_delay()
            return store_pb2.GetResponse(value=self.store[key], found=True)
        else:
            self._apply_delay()
            return store_pb2.GetResponse(found=False)
        
    def slowDown(self, request, context):
        sec = request.seconds/7
        self.delay = sec
        return store_pb2.SlowDownResponse(success=True)

    def restore(self, request, context):
        self.delay = 0
        return store_pb2.RestoreResponse(success=True)
        
    def canCommit(self, request, context):
        key = request.key
        value = request.value

        for ip, port in slaves_info:
            with grpc.insecure_channel(f'{ip}:{port}') as channel:
                stub = store_pb2_grpc.KeyValueStoreStub(channel)
                response = stub.canCommitRemote(store_pb2.CanCommitRemoteRequest(key=key, value=value))
                if not response.vote:
                    self._apply_delay()
                    return store_pb2.CanCommitResponse(vote=False)

        self._apply_delay()
        return store_pb2.CanCommitResponse(vote=True)

    def canCommitRemote(self, request, context):
        key = request.key
        value = request.value
        self._apply_delay()
        return store_pb2.CanCommitResponse(vote=True)
    
    def doCommit(self, request, context):
        key = request.key
        value = request.value
        
        # enviem sol.licituds rpc
        for i, (ip, port) in enumerate(slaves_info, start=1):
            with grpc.insecure_channel(f'{ip}:{port}') as channel:
                stub = store_pb2_grpc.KeyValueStoreStub(channel)
                response = stub.doCommitRemote(store_pb2.DoCommitRemoteRequest(key=key, value=value))
                if not response.success:
                    self.doAbort(request, context)
                    self._apply_delay()
                    return store_pb2.DoCommitResponse(success=False)
        self.store[key] = value
        self._save_data_to_yaml()
        self._apply_delay()
        return store_pb2.DoCommitResponse(success=True)

    def doCommitRemote(self, request, context):
        key = request.key
        value = request.value
        self.store[key] = value
        self._save_data_to_yaml()
        self._apply_delay()
        return store_pb2.DoCommitRemoteResponse(success=True)
    
    def doAbort(self, request, context):
        self._apply_delay()
        return store_pb2.DoAbortResponse(success=False)
    
    def haveCommitted(self, request, context):
        self._apply_delay()
        key = request.key
        value = request.value
        self._apply_delay()
        return store_pb2.HaveCommittedResponse(success=True)
    
    def _apply_delay(self):
        if self.delay > 0:
            time.sleep(self.delay)

    def _load_data_from_yaml(self):
        try:
            with open(f'../recovery/node_{self.node_id}_data.yaml', 'r') as file:
                yaml_data = file.read()
            self.store = yaml.safe_load(yaml_data)
        except FileNotFoundError:
            pass

    def _save_data_to_yaml(self):
        yaml_data = yaml.dump(self.store)
        with open(f'../recovery/node_{self.node_id}_data.yaml', 'w') as file:
            file.write(yaml_data)



def master(node_id):
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreService(node_id), server)
    server.add_insecure_port(f'{master_ip}:{master_port}')
    server.start()
    server.wait_for_termination()

def slave(node_id, ip, port):
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreService(node_id), server)
    server.add_insecure_port(f'{ip}:{port}')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    #Inicialitzem primer al node master
    thread_master = threading.Thread(target=master, args=(0,))
    thread_master.start()

    #Inicialitzem els nodes esclaus
    threads_slave = []
    for i, (ip, port) in enumerate(slaves_info, start=1):
        thread_slave = threading.Thread(target=slave, args=(i, ip, port))
        thread_slave.start()
        threads_slave.append(thread_slave)

    #Esperem a que els threads acabin
    thread_master.join()
    for thread in threads_slave:
        thread.join()