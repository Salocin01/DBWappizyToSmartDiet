#!/usr/bin/env python3

import os
import shutil
import subprocess
import tarfile
from datetime import datetime
from pathlib import Path
import paramiko
from dotenv import load_dotenv

load_dotenv()

class MongoRefreshManager:
    def __init__(self):
        self.remote_url = os.getenv('REMOTE_SERVER_URL')
        self.remote_user = os.getenv('REMOTE_SERVER_USER')
        self.remote_password = os.getenv('REMOTE_SERVER_PASSWORD')
        self.remote_path = os.getenv('REMOTE_MONGODB_PATH', '/tmp')
        self.remote_mongo_url = os.getenv('REMOTE_MONGODB_URL')
        self.remote_mongo_db = os.getenv('REMOTE_MONGODB_DATABASE')
        self.mongo_url = os.getenv('MONGODB_URL')
        self.mongo_db = os.getenv('MONGODB_DATABASE')
        self.tmp_dir = Path('./tmp')
        
        # Generate unique dump filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.dump_filename = f"mongo_dump_{timestamp}.tar.gz"
        self.remote_dump_path = f"{self.remote_path}/{self.dump_filename}"
        
        if not all([self.remote_url, self.remote_user, self.remote_mongo_url, self.remote_mongo_db, self.mongo_url, self.mongo_db]):
            raise ValueError("Missing required environment variables")
    
    def log_progress(self, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")
    
    def setup_tmp_directory(self):
        self.log_progress("Setting up temporary directory...")
        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir)
        self.tmp_dir.mkdir(exist_ok=True)
        self.log_progress("✓ Temporary directory ready")
    
    def connect_to_server(self):
        self.log_progress(f"Connecting to remote server {self.remote_url}...")
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(self.remote_url, username=self.remote_user, password=self.remote_password)
            self.sftp = self.ssh.open_sftp()
            self.log_progress("✓ Connected to remote server")
            return True
        except Exception as e:
            self.log_progress(f"✗ Failed to connect: {e}")
            return False
    
    def create_remote_dump(self):
        self.log_progress(f"Creating MongoDB dump on remote server...")
        try:
            # Create mongodump command
            dump_dir = f"{self.remote_path}/dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            mongodump_cmd = f"mongodump --uri='{self.remote_mongo_url}' --db={self.remote_mongo_db} --out={dump_dir}"
            
            # Execute mongodump on remote server
            stdin, stdout, stderr = self.ssh.exec_command(mongodump_cmd)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status != 0:
                error_msg = stderr.read().decode()
                self.log_progress(f"✗ Mongodump failed: {error_msg}")
                return False
            
            self.log_progress("✓ MongoDB dump created successfully")
            
            # Create tar.gz archive of the dump
            tar_cmd = f"cd {self.remote_path} && tar -czf {self.dump_filename} {os.path.basename(dump_dir)}"
            stdin, stdout, stderr = self.ssh.exec_command(tar_cmd)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status != 0:
                error_msg = stderr.read().decode()
                self.log_progress(f"✗ Tar creation failed: {error_msg}")
                return False
            
            self.log_progress("✓ Dump archived successfully")
            
            # Clean up the uncompressed dump directory
            cleanup_cmd = f"rm -rf {dump_dir}"
            self.ssh.exec_command(cleanup_cmd)
            
            self.dump_dir = dump_dir
            return True
        except Exception as e:
            self.log_progress(f"✗ Failed to create dump: {e}")
            return False
    
    def download_backup(self):
        local_file = self.tmp_dir / self.dump_filename
        self.log_progress(f"Downloading {self.dump_filename}...")
        try:
            self.sftp.get(self.remote_dump_path, str(local_file))
            self.local_backup_path = local_file
            self.log_progress(f"✓ Downloaded to {local_file}")
            return True
        except Exception as e:
            self.log_progress(f"✗ Download failed: {e}")
            return False
    
    def extract_backup(self):
        self.log_progress("Extracting backup archive...")
        try:
            with tarfile.open(self.local_backup_path, 'r:gz') as tar_ref:
                tar_ref.extractall(self.tmp_dir)
            self.log_progress("✓ Backup extracted successfully")
            return True
        except Exception as e:
            self.log_progress(f"✗ Extraction failed: {e}")
            return False
    
    def drop_existing_database(self):
        self.log_progress(f"Dropping existing database: {self.mongo_db}")
        try:
            cmd = ["mongosh", self.mongo_url, "--eval", f"db.getSiblingDB('{self.mongo_db}').dropDatabase()"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                self.log_progress("✓ Database dropped successfully")
                return True
            else:
                self.log_progress(f"✗ Failed to drop database: {result.stderr}")
                return False
        except Exception as e:
            self.log_progress(f"✗ Database drop error: {e}")
            return False
    
    def restore_database(self):
        self.log_progress(f"Restoring database from backup...")
        try:
            dump_dir = None
            for item in self.tmp_dir.iterdir():
                if item.is_dir() and item.name != '__MACOSX':
                    dump_dir = item
                    break
            
            if not dump_dir:
                self.log_progress("✗ No dump directory found in extracted files")
                return False
            
            # Look for the actual database directory inside the dump directory
            db_dump_dir = dump_dir / self.mongo_db
            if not db_dump_dir.exists():
                self.log_progress(f"✗ Database directory {self.mongo_db} not found in dump")
                return False
            
            cmd = ["mongorestore", "--uri", self.mongo_url, "--db", self.mongo_db, str(db_dump_dir)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.log_progress("✓ Database restored successfully")
                return True
            else:
                self.log_progress(f"✗ Restore failed: {result.stderr}")
                return False
        except Exception as e:
            self.log_progress(f"✗ Restore error: {e}")
            return False
    
    def cleanup_remote_dump(self):
        self.log_progress("Cleaning up remote dump file...")
        try:
            # Remove the zip file from remote server
            cleanup_cmd = f"rm -f {self.remote_dump_path}"
            stdin, stdout, stderr = self.ssh.exec_command(cleanup_cmd)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status == 0:
                self.log_progress("✓ Remote dump file cleaned up")
            else:
                error_msg = stderr.read().decode()
                self.log_progress(f"⚠ Remote cleanup warning: {error_msg}")
        except Exception as e:
            self.log_progress(f"⚠ Remote cleanup warning: {e}")
    
    def cleanup(self):
        self.log_progress("Cleaning up temporary files...")
        try:
            if hasattr(self, 'sftp'):
                self.sftp.close()
            if hasattr(self, 'ssh'):
                self.ssh.close()
            
            if self.tmp_dir.exists():
                shutil.rmtree(self.tmp_dir)
            
            self.log_progress("✓ Cleanup completed")
        except Exception as e:
            self.log_progress(f"⚠ Cleanup warning: {e}")
    
    def refresh_database(self):
        self.log_progress("🚀 Starting MongoDB database refresh process...")
        
        try:
            self.setup_tmp_directory()

            if not self.connect_to_server():
                return False
            
            if not self.create_remote_dump():
                return False
            
            if not self.download_backup():
                return False
            
            if not self.extract_backup():
                return False
            
            if not self.drop_existing_database():
                return False
            
            if not self.restore_database():
                return False
            
            # Clean up the remote dump file
            self.cleanup_remote_dump()
            
            self.log_progress("🎉 Database refresh completed successfully!")
            self.cleanup()
            return True
        
            
        except Exception as e:
            self.log_progress(f"✗ Process failed: {e}")
            return False
        finally:
            # Only close connections, don't remove tmp files on failure
            try:
                if hasattr(self, 'sftp'):
                    self.sftp.close()
                if hasattr(self, 'ssh'):
                    self.ssh.close()
            except Exception as e:
                self.log_progress(f"⚠ Connection cleanup warning: {e}")

def main():
    try:
        manager = MongoRefreshManager()
        success = manager.refresh_database()
        exit(0 if success else 1)
    except Exception as e:
        print(f"Fatal error: {e}")
        exit(1)

if __name__ == "__main__":
    main()