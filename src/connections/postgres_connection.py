import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

class PostgresConnection:
    def __init__(self):
        self.connection_pool = None
        
    def get_connection_params(self):
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

def connect_postgres():
    params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DATABASE', 'database'),
        'user': os.getenv('POSTGRES_USER', 'user'),
        'password': os.getenv('POSTGRES_PASSWORD', '')
    }
    return psycopg2.connect(**params)

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