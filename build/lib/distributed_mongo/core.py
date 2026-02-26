import random
import time
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from pymongo.server_api import ServerApi

STORAGE_LIMIT_BYTES = 450 * 1024 * 1024
UPDATE_INTERVAL = 1000
MAX_RETRIES = 5
BASE_BACKOFF = 2


def get_collection_size(collection):
    stats = collection.database.command("collStats", collection.name)
    return stats.get("size", 0)


class DistributedMongoDB:
    def __init__(self, servers, database_name, collection_name, type=None):
        self.clients = []
        self.collections = []
        self.size_cache = []
        self.operation_count = []
        self.database_name = database_name
        self.collection_name = collection_name
        self.server_uris = set()
        self.type = type

        for server in servers:
            self._add_server(server)

    def _add_server(self, server):
        client = MongoClient(server, server_api=ServerApi("1"))
        collection = client[self.database_name][self.collection_name]
        self.clients.append(client)
        self.collections.append(collection)
        self.size_cache.append(get_collection_size(collection))
        self.operation_count.append(0)
        self.server_uris.add(server)
        print("Added server", tag="INIT")

    def _update_size_cache(self, index, increment):
        self.size_cache[index] += increment
        self.operation_count[index] += 1
        if self.operation_count[index] >= UPDATE_INTERVAL:
            self.size_cache[index] = get_collection_size(self.collections[index])
            self.operation_count[index] = 0

    def find(self, query=None, projection=None):
        query = query or {}
        projection = projection or {}
        results = []
        for collection in self.collections:
            results.extend(collection.find(query, projection))
        return results

    def find_one(self, primary_key, value):
        for collection in self.collections:
            found = collection.find_one({primary_key: value})
            if found:
                return found
        return None

    def close_connection(self):
        for client in self.clients:
            client.close()
