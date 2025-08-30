#!/usr/bin/env python3

import os
import shutil
import subprocess
import zipfile
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
        self.remote_path = os.getenv('REMOTE_MONGODB_PATH')
        self.mongo_url = os.getenv('MONGODB_URL')
        self.mongo_db = os.getenv('MONGODB_DATABASE')
        self.tmp_dir = Path('./tmp')
        
        if not all([self.remote_url, self.remote_user, self.remote_path, self.mongo_url, self.mongo_db]):
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
    
    def find_latest_backup(self):
        self.log_progress(f"Searching for latest backup in {self.remote_path}...")
        try:
            files = self.sftp.listdir_attr(self.remote_path)
            zip_bak_files = [f for f in files if f.filename.endswith('.zip.bak')]
            
            if not zip_bak_files:
                raise FileNotFoundError("No .zip.bak files found")
            
            latest_file = max(zip_bak_files, key=lambda x: x.st_mtime)
            self.latest_backup = latest_file.filename
            self.remote_file_path = f"{self.remote_path}/{self.latest_backup}"
            self.log_progress(f"✓ Found latest backup: {self.latest_backup}")
            return True
        except Exception as e:
            self.log_progress(f"✗ Failed to find backup: {e}")
            return False
    
    def download_backup(self):
        local_file = self.tmp_dir / self.latest_backup
        self.log_progress(f"Downloading {self.latest_backup}...")
        try:
            self.sftp.get(self.remote_file_path, str(local_file))
            self.local_backup_path = local_file
            self.log_progress(f"✓ Downloaded to {local_file}")
            return True
        except Exception as e:
            self.log_progress(f"✗ Download failed: {e}")
            return False
    
    def extract_backup(self):
        self.log_progress("Extracting backup archive...")
        try:
            with zipfile.ZipFile(self.local_backup_path, 'r') as zip_ref:
                zip_ref.extractall(self.tmp_dir)
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
            
            if not self.find_latest_backup():
                return False
            
            if not self.download_backup():
                return False
            
            if not self.extract_backup():
                return False
            
            if not self.drop_existing_database():
                return False
            
            if not self.restore_database():
                return False
            
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