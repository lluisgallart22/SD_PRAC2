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

config_path = os.path.join(os.path.dirname(__file__), 'configuration', 'decentralized_config.yaml')
#Ports disponibles
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

nodes = config.get('nodes', [])
nodes_info = [(node['ip'], node['port']) for node in nodes]

#Implementació del KeyValueStore
class KeyValueStoreService(store_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, node_id, peers):
        self.store = {}
        self.delay = 0
        self.node_id = node_id
        self.peers = peers
        self.weights = {0: 1, 1: 2, 2: 1}
        self.lock = threading.Lock()
        self._load_data_from_yaml()

    def put(self, request, context):
        key = request.key
        value = request.value
        votes = self._gather_votes(key)
        total_votes = sum(int(vot) for vot in votes)
        if total_votes >= 3:
            success = self._commit_value(key, value)
            self._apply_delay()
            return store_pb2.PutResponse(success=success)
        else:
            self._apply_delay()
            return store_pb2.PutResponse(success=False)
        
    def get(self, request, context):
        key = request.key
        votes = self._gather_votes(key)
        total_votes = sum(int(vot) for vot in votes)
        if total_votes >= 2:
            if key in self.store:
                self._apply_delay()
                return store_pb2.GetResponse(value=self.store[key], found=True)
            else:
                self._apply_delay()
                return store_pb2.GetResponse(found=False)
        else:
            self._apply_delay()
            return store_pb2.GetResponse(found=False)
        
    def slowDown(self, request, context):
        sec = request.seconds/4
        self.delay = sec
        if self.delay != 0:
            return store_pb2.SlowDownResponse(success=True)
        else:
            return store_pb2.SlowDownResponse(success=False)
    
    
    def restore (self, request, context):
        self.delay = 0
        if self.delay == 0:
            return store_pb2.RestoreResponse(success=True)
        else:
            return store_pb2.RestoreResponse(success=False)

        
    def askVote(self, request, context):
       key = request.key
       vote = self.weights[self.node_id]
       self._apply_delay()
       return store_pb2.VoteResponse(vote=vote)
    
    def doCommit(self, request, context):
        key = request.key
        value = request.value
        if not key or not value:
            return store_pb2.DoCommitResponse(success=False)
        self.store[key] = value
        self._save_data_to_yaml()
        self._apply_delay()
        return store_pb2.DoCommitResponse(success=True)
    
    def _apply_delay(self):
        if self.delay > 0:
            time.sleep(self.delay)

    def _gather_votes(self, key):
        votes = []
        votes.append(self.weights[self.node_id])
        for peer in self.peers:
            with grpc.insecure_channel(peer) as channel:
                stub = store_pb2_grpc.KeyValueStoreStub(channel)
                response = stub.askVote(store_pb2.VoteRequest(key=key))
                votes.append(response.vote)
        return votes
    
    def _commit_value(self, key, value):
        success = True
        request = store_pb2.DoCommitRequest(key=key, value=value)
        for peer in self.peers:
            with grpc.insecure_channel(peer) as channel:
                stub = store_pb2_grpc.KeyValueStoreStub(channel)
                response = stub.doCommit(request)
                if not response.success:
                    success = False
        # Commit local store directly without calling doCommit
        with self.lock:
            self.store[key] = value
            self._save_data_to_yaml()
        return success

    def _load_data_from_yaml(self):
        try:
            with open(f'../recovery/node_{self.node_id}_decentralized_data.yaml', 'r') as file:
                yaml_data = file.read()
            self.store = yaml.safe_load(yaml_data)
        except FileNotFoundError:
            pass

    def _save_data_to_yaml(self):
        yaml_data = yaml.dump(self.store)
        with open(f'../recovery/node_{self.node_id}_decentralized_data.yaml', 'w') as file:
            file.write(yaml_data)
    
def nodesF(node_id, ip, port, peers):
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreService(node_id, peers), server)
    server.add_insecure_port(f'{ip}:{port}')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    threads = []
    for i, (ip, port) in enumerate(nodes_info):
        peers = [f"{peer_ip}:{peer_port}" for j, (peer_ip, peer_port) in enumerate(nodes_info) if i != j]
        thread = threading.Thread(target=nodesF, args=(i, ip, port, peers))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()