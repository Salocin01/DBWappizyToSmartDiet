import os
import pymysql
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

load_dotenv()


class MariaDBConnection:
    """MariaDB connection manager with optional SSH tunnel support"""

    def __init__(self):
        self.connection = None
        self.ssh_tunnel = None

    def get_connection_params(self):
        """Get MariaDB connection parameters based on transfer mode"""
        transfer_source = os.getenv('MATOMO_SOURCE', 'local').lower()

        if transfer_source == 'remote':
            # Use remote MariaDB configuration with SSH tunnel
            print(f"🌐 Setting up REMOTE MariaDB connection via SSH tunnel...")

            remote_server = os.getenv('REMOTE_SERVER_URL')
            remote_user = os.getenv('REMOTE_SERVER_USER')
            remote_password = os.getenv('REMOTE_SERVER_PASSWORD')
            remote_db = os.getenv('MATOMO_DATABASE', 'matomo')
            remote_db_user = os.getenv('MATOMO_USER', 'root')
            remote_db_password = os.getenv('MATOMO_PASSWORD', '')
            remote_port = int(os.getenv('MATOMO_PORT', '3306'))

            if not all([remote_server, remote_user, remote_db]):
                raise ValueError("Missing remote MariaDB configuration. Check REMOTE_SERVER_URL, REMOTE_SERVER_USER, MATOMO_DATABASE")

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
                'port': self.ssh_tunnel.local_bind_port,
                'database': remote_db,
                'user': remote_db_user,
                'password': remote_db_password,
                'charset': 'utf8mb4',
                'cursorclass': pymysql.cursors.DictCursor
            }
        else:
            # Use local MariaDB configuration (default)
            print(f"💻 Connecting to LOCAL MariaDB: {os.getenv('MATOMO_DATABASE', 'matomo')}")
            return {
                'host': os.getenv('MATOMO_HOST', 'localhost'),
                'port': int(os.getenv('MATOMO_PORT', '3306')),
                'database': os.getenv('MATOMO_DATABASE', 'matomo'),
                'user': os.getenv('MATOMO_USER', 'root'),
                'password': os.getenv('MATOMO_PASSWORD', ''),
                'charset': 'utf8mb4',
                'cursorclass': pymysql.cursors.DictCursor
            }

    def connect(self):
        """Create and return a MariaDB connection"""
        if not self.connection or not self.connection.open:
            params = self.get_connection_params()
            self.connection = pymysql.connect(**params)
        return self.connection

    def close(self):
        """Close MariaDB connection and SSH tunnel if applicable"""
        if self.connection and self.connection.open:
            self.connection.close()
            print("   → MariaDB connection closed")

        if self.ssh_tunnel:
            self.ssh_tunnel.stop()
            self.ssh_tunnel = None
            print("   → SSH tunnel closed")


# Global instance for connection management
_mariadb_connection_instance = None


def get_mariadb_connection():
    """Get or create a global MariaDB connection instance"""
    global _mariadb_connection_instance
    if not _mariadb_connection_instance:
        _mariadb_connection_instance = MariaDBConnection()
    return _mariadb_connection_instance.connect()


def close_mariadb_connection():
    """Close the global MariaDB connection and SSH tunnel"""
    global _mariadb_connection_instance
    if _mariadb_connection_instance:
        _mariadb_connection_instance.close()
        _mariadb_connection_instance = None
