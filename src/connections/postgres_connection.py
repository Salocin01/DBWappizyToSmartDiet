import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

load_dotenv()

class PostgresConnection:
    def __init__(self):
        self.connection_pool = None
        self.ssh_tunnel = None

    def get_connection_params(self):
        transfer_destination = os.getenv('TRANSFER_DESTINATION', 'local').lower()

        if transfer_destination == 'remote':
            # Use remote PostgreSQL configuration with SSH tunnel
            print(f"🌐 Setting up REMOTE PostgreSQL connection via SSH tunnel...")

            remote_server = os.getenv('REMOTE_SERVER_URL')
            remote_user = os.getenv('REMOTE_SERVER_USER')
            remote_password = os.getenv('REMOTE_SERVER_PASSWORD')
            remote_db = os.getenv('REMOTE_POSTGRES_DATABASE', 'database')
            remote_db_user = os.getenv('REMOTE_POSTGRES_USER', 'postgres')
            remote_db_password = os.getenv('REMOTE_POSTGRES_PASSWORD', '')
            remote_port = int(os.getenv('REMOTE_POSTGRES_PORT', '5432'))

            if not all([remote_server, remote_user, remote_db]):
                raise ValueError("Missing remote PostgreSQL configuration. Check REMOTE_SERVER_URL, REMOTE_SERVER_USER, REMOTE_POSTGRES_DATABASE")

            # Create SSH tunnel if not already created
            if not self.ssh_tunnel:
                self.ssh_tunnel = SSHTunnelForwarder(
                    (remote_server, 22),
                    ssh_username=remote_user,
                    ssh_password=remote_password,
                    remote_bind_address=('localhost', remote_port),
                    local_bind_address=('127.0.0.1', 0)  # Use any available local port
                )
                self.ssh_tunnel.start()
                local_port = self.ssh_tunnel.local_bind_port
                print(f"   → SSH tunnel established: localhost:{local_port} → {remote_server}:{remote_port}")
                print(f"   → Database: {remote_db}")

            return {
                'host': '127.0.0.1',
                'port': str(self.ssh_tunnel.local_bind_port),
                'database': remote_db,
                'user': remote_db_user,
                'password': remote_db_password
            }
        else:
            # Use local PostgreSQL configuration (default)
            print(f"💻 Connecting to LOCAL PostgreSQL: {os.getenv('POSTGRES_DATABASE', 'database')}")
            return {
                'host': os.getenv('POSTGRES_HOST', 'localhost'),
                'port': os.getenv('POSTGRES_PORT', '5432'),
                'database': os.getenv('POSTGRES_DATABASE', 'database'),
                'user': os.getenv('POSTGRES_USER', 'user'),
                'password': os.getenv('POSTGRES_PASSWORD', '')
            }
    
    def create_connection_pool(self, minconn=1, maxconn=10):
        params = self.get_connection_params()
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn, maxconn, **params
        )
        return self.connection_pool
    
    def get_connection(self):
        if not self.connection_pool:
            self.create_connection_pool()
        return self.connection_pool.getconn()
    
    def return_connection(self, conn):
        if self.connection_pool:
            self.connection_pool.putconn(conn)
    
    def close_all_connections(self):
        if self.connection_pool:
            self.connection_pool.closeall()
        if self.ssh_tunnel:
            self.ssh_tunnel.stop()
            self.ssh_tunnel = None
            print("   → SSH tunnel closed")

def connect_postgres():
    # Create a PostgresConnection instance to leverage SSH tunnel logic
    pg_conn = PostgresConnection()
    params = pg_conn.get_connection_params()

    # Store the connection instance globally so we can close the tunnel later
    global _pg_connection_instance
    _pg_connection_instance = pg_conn

    return psycopg2.connect(**params)

# Global instance to manage SSH tunnel lifecycle
_pg_connection_instance = None

def close_postgres_connection():
    """Close PostgreSQL connection and SSH tunnel if applicable"""
    global _pg_connection_instance
    if _pg_connection_instance:
        _pg_connection_instance.close_all_connections()
        _pg_connection_instance = None

def setup_tables(conn):
    try:
        from src.schemas.schemas import TABLE_SCHEMAS
        
        cursor = conn.cursor()
        print("PostgreSQL connected")
        
        # Sort tables by export_order, same as data import
        sorted_tables = sorted(TABLE_SCHEMAS.items(), key=lambda x: x[1].export_order)
        
        for table_name, schema in sorted_tables:
            cursor.execute(schema.get_create_sql())
            print(f"✅ Table {table_name} created/verified")
        
        conn.commit()
        print("All tables created")
        return conn
    except psycopg2.OperationalError as e:
        print(f"ERROR: {e}")
        raise