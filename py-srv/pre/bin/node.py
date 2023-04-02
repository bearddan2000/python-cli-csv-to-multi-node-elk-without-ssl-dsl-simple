import logging
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

from schema import configurations
from data import INDEX_NAME

logging.basicConfig(level=logging.INFO)

class Cluster():
    def __init__(self) -> None:
        self.hive = [
            Node("es1"),
            Node('es2'),
            Node('es3')
        ]
    
class Node():
    def __init__(self,server) -> None:
        self.server = server

        es = Elasticsearch(
            server + ":9200",
            http_auth=["elastic", "changeme"],
        )

        client = IndicesClient(es)

        try:
            client.create(index=INDEX_NAME, body=configurations)
        except:
            logging.warn(server + " already has index")
            pass

