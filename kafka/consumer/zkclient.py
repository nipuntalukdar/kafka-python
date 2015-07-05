''' Client class for Zookeeper '''

import logging
from functools import wraps
import json
from uuid import uuid1
from threading import Lock
from kazoo.client import KazooClient, KazooState
from time import sleep
from nsclient import nsclient
from kafka.consumer import SimpleConsumer
import sys


def synchronized(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.lock:
            func(self, *args, **kwargs)
    return wrapper

class kafka_consts(object):
    BROKER_ID_PATH = '/brokers/ids'
    CONSUMER_GROUP = 'apiconsumers'
    CONSUMER_PATH = '/consumers'

class zk_states_watcher:
    def __init__(self):
        self.state = KazooState.LOST

    def __call__(self, state):
        logging.info('New client state %s', state)
        self.state = state
    
    def current_state(self):
        return self.state

    def connected(self):
        return self.state == KazooState.CONNECTED

class zk_client:
    def __init__(self, topics, zk_hosts='127.0.0.1:2181', 
            consumer_group=kafka_consts.CONSUMER_GROUP):
        self.zk_hosts = zk_hosts
        self.kafka_client = None
        self.consumer_group = consumer_group
        self.lock = Lock()
        self.zk_st_watcher = zk_states_watcher()
        self.consumer_id = uuid1().hex
        self.consumer_ids = [self.consumer_id]
        self.consumer_id_path = '{}/{}/{}'.format(kafka_consts.CONSUMER_PATH, self.consumer_group,
                'ids')
        try:
            self.zoo_cl = KazooClient(self.zk_hosts)
            self.zoo_cl.add_listener(self.zk_st_watcher)
            self.broker_details = {}
            self.zoo_cl.start()
            sleep(1)
            self._init(topics)

        except Exception as e:
            logging.exception(e)

    def register(self):
        ret = False
        while not ret:
            ret = self.create_ephemeralpath(self.consumer_id_path + '/' + self.consumer_id)
            if not ret:
                sleep(1)
        
    def _init(self, topics):
        ret = False
        while not ret:
            ret = self.create_newpath(kafka_consts.CONSUMER_PATH + '/' + self.consumer_group)
            if not ret:
                sleep(1)
        ret = False
        while not ret:
            ret = self.create_newpath(kafka_consts.CONSUMER_PATH + '/' + self.consumer_group +
                    '/ids')
            if not ret:
                sleep(1)
       
        self.register()
        self.get_consumer_list()
        self.populate_broker_info()
        temptopics = [x.strip() for x in topics]
        self.topics = []
        for t in temptopics:
            if t != '' and t not in self.topics:
                self.topics.append(t)
        if not self.topics:
            raise ValueError('no topics passed')
        ret = False
        broker_ports = [] 
        with self.lock:
            for brid in self.broker_details:
                broker_port = self.broker_details[brid]
                broker_ports.append('{}:{}'.format(broker_port['host'],broker_port['port']))
        
        self.kafka_client = nsclient(broker_ports)
        self.topic_part_ids = {} 
        for topic in topics:
            pids = self.kafka_client.get_partition_ids_for_topic(topic)
            self.topic_part_ids[topic] = pids
        self.consumed = {} 
        self.rebalance_consumers()
        
        try:
            topic_partitions = {t : None for t in self.topics}
            self.kconsumer = SimpleConsumer(self.kafka_client, self.consumer_group, None,
                    topic_partitions=self.consumed.copy())
        except Exception as e:
            logging.exception(e)
            sys.exit(1)

    def get_message(self): 
        try:
            return self.kconsumer.get_message(timeout=1, get_partition_info=True)
        except Exception as e:
            logging.exception(e)
            return None
    
    @synchronized
    def populate_broker_info(self):
        brokers = self.get_brokerids()
        self.broker_details.clear()
        for brid in brokers:
            try:
                brdetails = self.get_data(kafka_consts.BROKER_ID_PATH + '/' + brid)
                if brdetails is None:
                    continue
                brjson = json.loads(brdetails[0])
                self.broker_details[brid] = brjson
            except Exception as e:
                logging.exception(e)

    def create_newpath(self, path):
        '''
        Create the znode path if it is not existing already
        '''
        try:
            if not self.zoo_cl.exists(path):
                self.zoo_cl.ensure_path(path)
        except Exception as e:
            logging.exception(e)
            return False
        return True
    
    def get_children(self, parentpath):
        try:
            children = self.zoo_cl.get_children(parentpath, watch=self)
            return children
        except Exception as e:
            logging.error(e)
            return None


    def get_brokerids(self):
        return self.get_children(kafka_consts.BROKER_ID_PATH)

    @synchronized
    def get_consumer_list(self):
        while True:
            print self.consumer_id_path 
            cids = self.get_children(self.consumer_id_path)
            if cids is None:
                sleep(1)
                continue
            self.consumer_ids = cids
            break
        self.consumer_ids.sort()
 
    def get_data(self, path):
        try:
            return self.zoo_cl.get(path)
        except Exception as e:
            logging.exception(e)
            return None

    def create_ephemeralpath(self, path):
        '''
        Create the znode ephemeral path if it is not existing
        '''
        try:
            if self.zoo_cl.exists(path):
                return True
            self.zoo_cl.create(path, ephemeral=True)
        except Exception as e:
            logging.exception(e)
            return False
        return True

    @synchronized
    def rebalance_consumers(self):
        self.topic_part_ids
        self.consumer_ids
        num_consumer = len(self.consumer_ids)
        cinsumerpos = self.consumer_ids.index(self.consumer_id)
        consumed_parts = {}
        for topic in self.topic_part_ids:
            partitions = filter(lambda x : x % num_consumer == 0, self.topic_part_ids[topic])
            consumed_parts[topic] = partitions
        self.consumed = consumed_parts
    
    @synchronized
    def print_brokers(self):
        print(self.broker_details)

    def __call__(self, event):
        if event.path == kafka_consts.BROKER_ID_PATH:
            self.populate_broker_info()
        elif event.path == self.consumer_id_path:
            self.rebalance_consumers()
            self.get_consumer_list()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    client = zk_client(['test', 'other'],zk_hosts='127.0.0.1:2181')
    #client.print_brokers()
    while True:
        minfo = client.get_message()
        if minfo is not None:
            topic, partition, message = minfo
            print topic, partition, minfo
