import os
from pymongo import MongoClient
from dotenv import load_dotenv
import paramiko
from sshtunnel import SSHTunnelForwarder

load_dotenv()

class MongoConnection:
    _instance = None
    _client = None
    _db = None
    _ssh_tunnel = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoConnection, cls).__new__(cls)
        return cls._instance

    def connect(self):
        if self._client is None:
            transfer_source = os.getenv('TRANSFER_SOURCE', 'local').lower()

            if transfer_source == 'remote':
                # Use remote MongoDB configuration with SSH tunnel
                print(f"🌐 Connecting to REMOTE MongoDB via SSH tunnel...")

                remote_server = os.getenv('REMOTE_SERVER_URL')
                remote_user = os.getenv('REMOTE_SERVER_USER')
                remote_password = os.getenv('REMOTE_SERVER_PASSWORD')
                database_name = os.getenv('REMOTE_MONGODB_DATABASE', 'default')

                if not all([remote_server, remote_user, database_name]):
                    raise ValueError("Missing remote MongoDB configuration. Check REMOTE_SERVER_URL, REMOTE_SERVER_USER, REMOTE_MONGODB_DATABASE")

                # Parse MongoDB URL to get host and port
                mongo_url = os.getenv('REMOTE_MONGODB_URL', 'mongodb://localhost:27017')
                # Extract host and port from URL (assuming format mongodb://host:port)
                mongo_host = 'localhost'  # MongoDB on remote server
                mongo_port = 27017

                # Create SSH tunnel
                self._ssh_tunnel = SSHTunnelForwarder(
                    (remote_server, 22),
                    ssh_username=remote_user,
                    ssh_password=remote_password,
                    remote_bind_address=(mongo_host, mongo_port),
                    local_bind_address=('127.0.0.1', 0)  # Use any available local port
                )

                self._ssh_tunnel.start()
                local_port = self._ssh_tunnel.local_bind_port

                print(f"   → SSH tunnel established: localhost:{local_port} → {remote_server}:{mongo_port}")
                print(f"   → Database: {database_name}")

                # Connect to MongoDB through the tunnel
                mongodb_url = f'mongodb://127.0.0.1:{local_port}'

            else:
                # Use local MongoDB configuration (default)
                mongodb_url = os.getenv('MONGODB_URL', 'mongodb://localhost:27017')
                database_name = os.getenv('MONGODB_DATABASE', 'default')
                print(f"💻 Connecting to LOCAL MongoDB: {database_name}")

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
        if self._ssh_tunnel:
            self._ssh_tunnel.stop()
            self._ssh_tunnel = None
            print("   → SSH tunnel closed")

def get_mongo_db():
    mongo_conn = MongoConnection()
    return mongo_conn.connect()

def get_mongo_collection(collection_name):
    mongo_conn = MongoConnection()
    return mongo_conn.get_collection(collection_name)