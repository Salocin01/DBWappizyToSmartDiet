import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

class MongoConnection:
    _instance = None
    _client = None
    _db = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoConnection, cls).__new__(cls)
        return cls._instance
        
    def connect(self):
        if self._client is None:
            mongodb_url = os.getenv('MONGODB_URL', 'mongodb://localhost:27017')
            database_name = os.getenv('MONGODB_DATABASE', 'default')
            
            self._client = MongoClient(
                mongodb_url, 
                datetime_conversion='DATETIME_AUTO',
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=30000,
                socketTimeoutMS=30000
            )
            self._db = self._client[database_name]
        return self._db
    
    def get_collection(self, collection_name):
        if self._db is None:
            self.connect()
        return self._db[collection_name]
    
    def close(self):
        if self._client:
            self._client.close()
            self._client = None
            self._db = None

def get_mongo_db():
    mongo_conn = MongoConnection()
    return mongo_conn.connect()

def get_mongo_collection(collection_name):
    mongo_conn = MongoConnection()
    return mongo_conn.get_collection(collection_name)