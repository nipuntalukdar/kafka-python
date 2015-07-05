from kafka.client import KafkaClient
from kafka.conn import collect_hosts, KafkaConnection, DEFAULT_SOCKET_TIMEOUT_SECONDS


class nsclient(KafkaClient):
    CLIENT_ID = b'ns-kafka-python'

    def __init__(self, hosts, client_id=CLIENT_ID,
                 timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS,
                 correlation_id=0):
        KafkaClient.__init__(self, hosts, client_id, timeout, correlation_id)

