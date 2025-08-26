import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

class MongoConnection:
    def __init__(self):
        self.client = None
        self.db = None
        
    def connect(self):
        mongodb_url = os.getenv('MONGODB_URL', 'mongodb://localhost:27017')
        database_name = os.getenv('MONGODB_DATABASE', 'default')
        
        self.client = MongoClient(mongodb_url)
        self.db = self.client[database_name]
        return self.db
    
    def get_collection(self, collection_name):
        if not self.db:
            self.connect()
        return self.db[collection_name]
    
    def close(self):
        if self.client:
            self.client.close()

def get_mongo_db():
    mongo_conn = MongoConnection()
    return mongo_conn.connect()

def get_mongo_collection(collection_name):
    mongo_conn = MongoConnection()
    return mongo_conn.get_collection(collection_name)