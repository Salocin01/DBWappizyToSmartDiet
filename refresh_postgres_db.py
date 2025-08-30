#!/usr/bin/env python3
"""
PostgreSQL Database Refresh Manager

Optimized script to refresh PostgreSQL databases with support for:
- Selective table/schema filtering
- Data sampling for development databases
- Parallel compression with pigz
- Optimized pg_dump parameters

Environment Variables for Dump Optimization:
- POSTGRES_INCLUDE_TABLES: Comma-separated list of tables to include (e.g., "users,orders,products")
- POSTGRES_EXCLUDE_TABLES: Comma-separated list of tables to exclude (e.g., "logs,temp_table")
- POSTGRES_INCLUDE_SCHEMAS: Comma-separated list of schemas to include (e.g., "public,app")
- POSTGRES_EXCLUDE_SCHEMAS: Comma-separated list of schemas to exclude (e.g., "test,archive")
- POSTGRES_EXCLUDE_DATA_TABLES: Comma-separated list of tables to exclude data from (schema only)
- POSTGRES_DATA_SAMPLE_PERCENT: Percentage of data to sample (1-100, default: 100)
- MAX_DUMP_SIZE_MB: Maximum size in MB before splitting files (default: 100)

Examples:
- For development with sample data: POSTGRES_DATA_SAMPLE_PERCENT=10
- Exclude log tables: POSTGRES_EXCLUDE_TABLES="access_logs,error_logs,audit_trail"
- Include only specific tables: POSTGRES_INCLUDE_TABLES="users,products,orders"
"""

import os
import shutil
import subprocess
import gzip
import math
from datetime import datetime
from pathlib import Path
import paramiko
from dotenv import load_dotenv

load_dotenv()

class PostgresRefreshManager:
    def __init__(self):
        self.remote_url = os.getenv('REMOTE_SERVER_URL')
        self.remote_user = os.getenv('REMOTE_SERVER_USER')
        self.remote_password = os.getenv('REMOTE_SERVER_PASSWORD')
        self.remote_path = os.getenv('REMOTE_POSTGRES_PATH', '/home/wappizy/postgres/')
        
        self.local_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.local_port = os.getenv('POSTGRES_PORT', '5432')
        self.local_db = os.getenv('POSTGRES_DATABASE')
        self.local_user = os.getenv('POSTGRES_USER')
        self.local_password = os.getenv('POSTGRES_PASSWORD')
        
        self.remote_host = 'localhost'  # Use localhost when connecting via SSH
        self.remote_port = os.getenv('REMOTE_POSTGRES_PORT', '5432')
        self.remote_db = os.getenv('REMOTE_POSTGRES_DATABASE', 'smartdiet_app_clean')
        self.remote_db_user = os.getenv('REMOTE_POSTGRES_USER', self.local_user)
        self.remote_db_password = os.getenv('REMOTE_POSTGRES_PASSWORD', self.local_password)
        
        self.tmp_dir = Path('./tmp')
        self.max_file_size_mb = int(os.getenv('MAX_DUMP_SIZE_MB', '100'))
        
        # Dump optimization options
        self.include_tables = os.getenv('POSTGRES_INCLUDE_TABLES', '').strip()
        self.exclude_tables = os.getenv('POSTGRES_EXCLUDE_TABLES', '').strip()
        self.include_schemas = os.getenv('POSTGRES_INCLUDE_SCHEMAS', '').strip()
        self.exclude_schemas = os.getenv('POSTGRES_EXCLUDE_SCHEMAS', '').strip()
        self.exclude_data_tables = os.getenv('POSTGRES_EXCLUDE_DATA_TABLES', '').strip()
        self.data_sample_percentage = float(os.getenv('POSTGRES_DATA_SAMPLE_PERCENT', '100'))
        
        if not all([self.remote_url, self.remote_user, self.local_db, self.local_user]):
            raise ValueError("Missing required environment variables")
    
    def log_progress(self, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Ensure passwords are never logged
        safe_message = message
        if self.remote_db_password:
            safe_message = safe_message.replace(self.remote_db_password, '[PROTECTED]')
        if self.local_password:
            safe_message = safe_message.replace(self.local_password, '[PROTECTED]')
        if self.remote_password:
            safe_message = safe_message.replace(self.remote_password, '[PROTECTED]')
        print(f"[{timestamp}] {safe_message}")
    
    def setup_tmp_directory(self):
        self.log_progress("Setting up temporary directory...")
        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir)
        self.tmp_dir.mkdir(exist_ok=True)
        self.log_progress("✓ Temporary directory ready")
    
    def _build_dump_filters(self):
        """Build pg_dump filter arguments based on environment variables"""
        filters = []
        
        # Include specific tables
        if self.include_tables:
            for table in self.include_tables.split(','):
                table = table.strip()
                if table:
                    filters.extend(['--table', table])
        
        # Exclude specific tables
        if self.exclude_tables:
            for table in self.exclude_tables.split(','):
                table = table.strip()
                if table:
                    filters.extend(['--exclude-table', table])
        
        # Include specific schemas
        if self.include_schemas:
            for schema in self.include_schemas.split(','):
                schema = schema.strip()
                if schema:
                    filters.extend(['--schema', schema])
        
        # Exclude specific schemas
        if self.exclude_schemas:
            for schema in self.exclude_schemas.split(','):
                schema = schema.strip()
                if schema:
                    filters.extend(['--exclude-schema', schema])
        
        return filters
    
    def _build_data_filters(self):
        """Build pg_dump filter arguments for data-only dump"""
        filters = []
        
        # Exclude data from specific tables (schema only)
        if self.exclude_data_tables:
            for table in self.exclude_data_tables.split(','):
                table = table.strip()
                if table:
                    filters.extend(['--exclude-table-data', table])
        
        return filters
    
    def _create_sampled_data_dump(self, dump_file, env, data_filters):
        """Create a data dump with sampling for large tables"""
        try:
            # Get list of tables to sample
            tables_query = """
            SELECT schemaname, tablename, n_tup_ins + n_tup_upd + n_tup_del as total_operations
            FROM pg_stat_user_tables 
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
            ORDER BY total_operations DESC;
            """
            
            # Execute query to get table list
            import psycopg2
            conn = psycopg2.connect(
                host=self.local_host,
                port=self.local_port,
                database=self.local_db,
                user=self.local_user,
                password=self.local_password
            )
            
            cursor = conn.cursor()
            cursor.execute(tables_query)
            tables = cursor.fetchall()
            
            with open(dump_file, 'a') as f:
                f.write(f'\n\n-- Sampled data dump ({self.data_sample_percentage}%)\n')
                
                for schema, table, _ in tables:  # operations not used
                    qualified_table = f'"{schema}"."{table}"'
                    
                    # Skip tables in exclude list
                    if self.exclude_data_tables:
                        skip_table = False
                        for exclude_pattern in self.exclude_data_tables.split(','):
                            exclude_pattern = exclude_pattern.strip()
                            if exclude_pattern and (exclude_pattern == table or exclude_pattern == qualified_table):
                                skip_table = True
                                break
                        if skip_table:
                            continue
                    
                    # Get row count
                    cursor.execute(f'SELECT COUNT(*) FROM {qualified_table}')
                    row_count = cursor.fetchone()[0]
                    
                    if row_count == 0:
                        continue
                    
                    sample_size = max(1, int(row_count * self.data_sample_percentage / 100))
                    
                    if sample_size >= row_count:
                        # Export all data if sample size is >= total rows
                        self.log_progress(f"  → Exporting all {row_count} rows from {qualified_table}")
                        cursor.execute(f'SELECT * FROM {qualified_table}')
                    else:
                        # Use TABLESAMPLE for large tables (PostgreSQL 9.5+)
                        self.log_progress(f"  → Sampling {sample_size}/{row_count} rows from {qualified_table}")
                        cursor.execute(f'SELECT * FROM {qualified_table} TABLESAMPLE SYSTEM ({self.data_sample_percentage})')
                    
                    # Write COPY statement
                    f.write(f'\n-- Data for {qualified_table}\n')
                    f.write(f'TRUNCATE {qualified_table};\n')
                    
                    # Get column names
                    cursor.execute(f"""
                        SELECT column_name FROM information_schema.columns 
                        WHERE table_schema = '{schema}' AND table_name = '{table}'
                        ORDER BY ordinal_position
                    """)
                    columns = [row[0] for row in cursor.fetchall()]
                    column_list = ', '.join([f'"{col}"' for col in columns])
                    
                    f.write(f'COPY {qualified_table} ({column_list}) FROM stdin;\n')
                    
                    # Fetch and write data
                    cursor.execute(f'SELECT * FROM {qualified_table} TABLESAMPLE SYSTEM ({self.data_sample_percentage})' if sample_size < row_count else f'SELECT * FROM {qualified_table}')
                    
                    while True:
                        rows = cursor.fetchmany(1000)  # Process in batches
                        if not rows:
                            break
                        
                        for row in rows:
                            # Convert None to \N for COPY format
                            formatted_row = []
                            for value in row:
                                if value is None:
                                    formatted_row.append('\\N')
                                elif isinstance(value, str):
                                    # Escape special characters for COPY format
                                    value = value.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                                    formatted_row.append(value)
                                else:
                                    formatted_row.append(str(value))
                            
                            f.write('\t'.join(formatted_row) + '\n')
                    
                    f.write('\\.\n\n')
            
            cursor.close()
            conn.close()
            
            self.log_progress("✓ Sampled data dump completed")
            
        except Exception as e:
            self.log_progress(f"⚠ Sampling failed, falling back to regular dump: {e}")
            # Fallback to regular data dump
            data_cmd = [
                'pg_dump',
                '--host', self.local_host,
                '--port', self.local_port,
                '--username', self.local_user,
                '--dbname', self.local_db,
                '--data-only',
                '--no-owner',
                '--no-privileges',
                '--format=plain'
            ] + data_filters
            
            with open(dump_file, 'a') as f:
                f.write('\n\n-- Full data dump (sampling failed)\n')
                result = subprocess.run(data_cmd, stdout=f, 
                                      stderr=subprocess.PIPE, text=True, env=env)
            
            if result.returncode != 0:
                raise Exception(f"Fallback data dump failed: {result.stderr}")
    
    def create_local_dump(self):
        self.log_progress(f"Creating dump of local database: {self.local_db}")
        
        # Show active filters
        if self.include_tables or self.exclude_tables or self.include_schemas or self.exclude_schemas:
            self.log_progress("Active filters:")
            if self.include_tables:
                self.log_progress(f"  → Include tables: {self.include_tables}")
            if self.exclude_tables:
                self.log_progress(f"  → Exclude tables: {self.exclude_tables}")
            if self.include_schemas:
                self.log_progress(f"  → Include schemas: {self.include_schemas}")
            if self.exclude_schemas:
                self.log_progress(f"  → Exclude schemas: {self.exclude_schemas}")
            if self.exclude_data_tables:
                self.log_progress(f"  → Exclude data from: {self.exclude_data_tables}")
        
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            dump_file = self.tmp_dir / f"postgres_dump_{timestamp}.sql"
            
            # Set environment variables for pg_dump
            env = os.environ.copy()
            if self.local_password:
                env['PGPASSWORD'] = self.local_password
            
            # Build filter arguments
            schema_filters = self._build_dump_filters()
            data_filters = self._build_dump_filters() + self._build_data_filters()
            
            # Optimized schema dump command
            cmd = [
                'pg_dump',
                '--host', self.local_host,
                '--port', self.local_port,
                '--username', self.local_user,
                '--dbname', self.local_db,
                '--schema-only',
                '--no-owner',
                '--no-privileges',
                '--clean',
                '--if-exists',
                '--format=plain'
            ] + schema_filters
            
            self.log_progress("Creating optimized schema dump...")
            with open(dump_file, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, 
                                      text=True, env=env)
            
            if result.returncode != 0:
                self.log_progress(f"✗ Schema dump failed: {result.stderr}")
                return False
            
            # Optimized data dump - use COPY format for better performance and smaller size
            data_cmd = [
                'pg_dump',
                '--host', self.local_host,
                '--port', self.local_port,
                '--username', self.local_user,
                '--dbname', self.local_db,
                '--data-only',
                '--no-owner',
                '--no-privileges',
                '--format=plain'
            ] + data_filters
            
            # Add data sampling if configured
            if self.data_sample_percentage < 100:
                self.log_progress(f"Creating sampled data dump ({self.data_sample_percentage}%)...")
                # For sampling, we'll need to create a custom query approach
                self._create_sampled_data_dump(dump_file, env, data_filters)
            else:
                self.log_progress("Creating full data dump...")
                with open(dump_file, 'a') as f:
                    f.write('\n\n-- Data dump\n')
                    result = subprocess.run(data_cmd, stdout=f, 
                                          stderr=subprocess.PIPE, text=True, env=env)
                
                if result.returncode != 0:
                    self.log_progress(f"✗ Data dump failed: {result.stderr}")
                    return False
            
            self.dump_file = dump_file
            file_size_mb = dump_file.stat().st_size / (1024 * 1024)
            self.log_progress(f"✓ Optimized dump created: {dump_file} ({file_size_mb:.1f} MB)")
            
            return True
            
        except Exception as e:
            self.log_progress(f"✗ Dump creation failed: {e}")
            return False
    
    
    def split_dump_if_needed(self):
        if not hasattr(self, 'dump_file'):
            return False
            
        file_size_mb = self.dump_file.stat().st_size / (1024 * 1024)
        
        if file_size_mb <= self.max_file_size_mb:
            self.log_progress(f"Dump size ({file_size_mb:.1f} MB) is within limit")
            return True
        
        self.log_progress(f"Dump size ({file_size_mb:.1f} MB) exceeds limit, splitting...")
        
        try:
            with open(self.dump_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Calculate target number of chunks
            target_chunks = math.ceil(file_size_mb / self.max_file_size_mb)
            target_size = len(content) // target_chunks
            
            self.log_progress(f"Target: {target_chunks} chunks of ~{target_size/1024/1024:.1f} MB each")
            
            chunks = self._split_sql_content(content, target_size)
            
            if not chunks:
                self.log_progress("✗ Failed to split SQL content")
                return False
            
            # Create split files
            self.split_files = []
            for i, chunk in enumerate(chunks):
                split_file = self.tmp_dir / f"{self.dump_file.stem}_part_{i+1:03d}.sql"
                with open(split_file, 'w', encoding='utf-8') as f:
                    f.write(chunk)
                
                chunk_size_mb = len(chunk.encode('utf-8')) / (1024 * 1024)
                self.split_files.append(split_file)
                self.log_progress(f"  Created part {i+1}: {chunk_size_mb:.1f} MB")
            
            # Remove original file
            self.dump_file.unlink()
            self.log_progress(f"✓ Split into {len(self.split_files)} files")
            return True
            
        except Exception as e:
            self.log_progress(f"✗ File splitting failed: {e}")
            return False
    
    def _split_sql_content(self, content, target_size):
        """Split SQL content at proper statement boundaries"""
        chunks = []
        lines = content.split('\n')
        current_chunk = []
        current_size = 0
        in_function = False
        in_copy = False
        function_depth = 0
        
        for line in lines:
            line_with_newline = line + '\n'
            line_size = len(line_with_newline.encode('utf-8'))
            
            # Track function boundaries (PostgreSQL functions can have nested semicolons)
            if self._is_function_start(line):
                in_function = True
                function_depth = 1
            elif in_function:
                if '$$' in line or 'END;' in line.upper() or 'END ' in line.upper():
                    function_depth -= 1
                    if function_depth <= 0:
                        in_function = False
                elif 'BEGIN' in line.upper():
                    function_depth += 1
            
            # Track COPY statements
            if line.strip().startswith('COPY ') or line.strip().startswith('\\copy '):
                in_copy = True
            elif in_copy and line.strip() == '\\.':
                in_copy = False
            
            current_chunk.append(line_with_newline)
            current_size += line_size
            
            # Check if we should split here
            should_split = (
                current_size >= target_size and
                not in_function and
                not in_copy and
                self._is_safe_split_point(line)
            )
            
            if should_split:
                chunk_content = ''.join(current_chunk)
                if chunk_content.strip():  # Only add non-empty chunks
                    chunks.append(chunk_content)
                current_chunk = []
                current_size = 0
        
        # Add remaining content
        if current_chunk:
            chunk_content = ''.join(current_chunk)
            if chunk_content.strip():
                chunks.append(chunk_content)
        
        return chunks
    
    def _is_function_start(self, line):
        """Check if line starts a PostgreSQL function/procedure"""
        upper_line = line.upper().strip()
        return (
            upper_line.startswith('CREATE OR REPLACE FUNCTION') or
            upper_line.startswith('CREATE FUNCTION') or
            upper_line.startswith('CREATE OR REPLACE PROCEDURE') or
            upper_line.startswith('CREATE PROCEDURE') or
            upper_line.startswith('CREATE TRIGGER') or
            upper_line.startswith('CREATE OR REPLACE TRIGGER')
        )
    
    def _is_safe_split_point(self, line):
        """Check if this is a safe place to split SQL content"""
        stripped = line.strip()
        
        # Empty lines are always safe
        if not stripped:
            return True
        
        # Comments are safe
        if stripped.startswith('--'):
            return True
        
        # End of statements
        if stripped.endswith(';'):
            # But not if it's inside a string or function
            return True
        
        # Start of new major sections
        safe_starts = [
            'SET ', 'CREATE TABLE', 'CREATE INDEX', 'CREATE UNIQUE INDEX',
            'ALTER TABLE', 'INSERT INTO', 'UPDATE ', 'DELETE FROM',
            'DROP TABLE', 'DROP INDEX', 'TRUNCATE', 'ANALYZE',
            'VACUUM', 'COMMENT ON'
        ]
        
        return any(stripped.upper().startswith(start) for start in safe_starts)
    
    def compress_dumps(self):
        self.log_progress("Starting parallel compression of dump files...")
        try:
            files_to_compress = getattr(self, 'split_files', [self.dump_file])
            self.compressed_files = []
            
            # Use parallel compression for better performance
            import concurrent.futures
            import os
            
            def compress_file(dump_file):
                """Compress a single file using pigz if available, otherwise gzip"""
                if not dump_file.exists():
                    return None, f"File {dump_file.name} does not exist"
                
                original_size = dump_file.stat().st_size / (1024 * 1024)
                compressed_file = dump_file.with_suffix('.sql.gz')
                
                # Try to use pigz (parallel gzip) if available, otherwise use regular gzip
                try:
                    # Check if pigz is available
                    subprocess.run(['pigz', '--version'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
                    
                    # Use pigz for parallel compression
                    result = subprocess.run([
                        'pigz', '-9', '--force', '--keep', str(dump_file)
                    ], capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        # pigz creates .gz extension, rename to .sql.gz
                        gz_file = dump_file.with_suffix(dump_file.suffix + '.gz')
                        if gz_file.exists():
                            gz_file.rename(compressed_file)
                            dump_file.unlink()  # Remove original
                        compression_method = "pigz (parallel)"
                    else:
                        raise subprocess.CalledProcessError(result.returncode, 'pigz')
                        
                except (subprocess.CalledProcessError, FileNotFoundError):
                    # Fallback to regular gzip
                    with open(dump_file, 'rb') as f_in:
                        with gzip.open(compressed_file, 'wb', compresslevel=9) as f_out:
                            shutil.copyfileobj(f_in, f_out, length=65536)  # Use larger buffer
                    
                    dump_file.unlink()  # Remove original
                    compression_method = "gzip"
                
                compressed_size = compressed_file.stat().st_size / (1024 * 1024)
                compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
                
                return {
                    'file': compressed_file,
                    'original_size': original_size,
                    'compressed_size': compressed_size,
                    'compression_ratio': compression_ratio,
                    'method': compression_method
                }, None
            
            # Process files in parallel (limit to CPU count to avoid overwhelming the system)
            max_workers = min(len(files_to_compress), os.cpu_count() or 1)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all compression tasks
                future_to_file = {
                    executor.submit(compress_file, dump_file): dump_file 
                    for dump_file in files_to_compress
                }
                
                # Process completed tasks
                for i, future in enumerate(concurrent.futures.as_completed(future_to_file), 1):
                    dump_file = future_to_file[future]
                    
                    try:
                        result, error = future.result()
                        
                        if error:
                            self.log_progress(f"[{i}/{len(files_to_compress)}] ⚠ {error}")
                            continue
                        
                        self.compressed_files.append(result['file'])
                        self.log_progress(f"[{i}/{len(files_to_compress)}] ✓ {dump_file.name} compressed using {result['method']}: "
                                        f"{result['original_size']:.1f} MB → {result['compressed_size']:.1f} MB "
                                        f"({result['compression_ratio']:.1f}% reduction)")
                        
                    except Exception as e:
                        self.log_progress(f"[{i}/{len(files_to_compress)}] ✗ Error compressing {dump_file.name}: {e}")
                        return False
            
            if not self.compressed_files:
                self.log_progress("✗ No files were compressed successfully")
                return False
            
            total_original = sum(f.stat().st_size for f in files_to_compress if f.exists()) / (1024 * 1024)
            total_compressed = sum(f.stat().st_size for f in self.compressed_files) / (1024 * 1024)
            total_ratio = (1 - total_compressed / total_original) * 100 if total_original > 0 else 0
            
            self.log_progress(f"✓ Parallel compression completed: {len(self.compressed_files)} files ready")
            self.log_progress(f"  → Total: {total_original:.1f} MB → {total_compressed:.1f} MB ({total_ratio:.1f}% reduction)")
            return True
            
        except Exception as e:
            self.log_progress(f"✗ Compression failed: {e}")
            return False
    
    def connect_to_server(self):
        self.log_progress(f"Connecting to remote server {self.remote_url} as {self.remote_user}...")
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(self.remote_url, username=self.remote_user, 
                           password=self.remote_password)
            self.sftp = self.ssh.open_sftp()
            self.log_progress("✓ SSH connection established successfully")
            return True
        except Exception as e:
            # Ensure password is not exposed in connection errors
            safe_error = str(e).replace(self.remote_password, '[PROTECTED]') if self.remote_password else str(e)
            self.log_progress(f"✗ Failed to connect: {safe_error}")
            return False
    
    def upload_dumps(self):
        self.log_progress("Uploading dump files to remote server...")
        try:
            self.remote_files = []
            
            for local_file in self.compressed_files:
                remote_file = f"{self.remote_path}/{local_file.name}"
                self.sftp.put(str(local_file), remote_file)
                self.remote_files.append(remote_file)
                self.log_progress(f"✓ Uploaded {local_file.name}")
            
            return True
            
        except Exception as e:
            self.log_progress(f"✗ Upload failed: {e}")
            return False
    
    def create_remote_db_if_not_exists(self):
        """Create the database on remote server if it doesn't exist"""
        self.log_progress(f"Checking if database {self.remote_db} exists on remote server...")
        try:
            # First, connect to the default 'postgres' database to check if target database exists
            check_cmd = (f"export PGPASSWORD='{self.remote_db_password}' && "
                        f"psql -h {self.remote_host} -p {self.remote_port} "
                        f"-U {self.remote_db_user} -d postgres "
                        f"-t -c \"SELECT 1 FROM pg_database WHERE datname='{self.remote_db}';\"")
            
            self.log_progress(f"  → Checking: psql -h {self.remote_host} -p {self.remote_port} -U {self.remote_db_user} -d postgres -c \"SELECT 1 FROM pg_database WHERE datname='{self.remote_db}';\"")
            
            _, stdout, stderr = self.ssh.exec_command(check_cmd)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status != 0:
                error_msg = stderr.read().decode().strip()
                safe_error = error_msg.replace(self.remote_db_password, '[PROTECTED]') if self.remote_db_password else error_msg
                self.log_progress(f"  ✗ Failed to check database existence: {safe_error}")
                return False
            
            result = stdout.read().decode().strip()
            
            if result == '1':
                self.log_progress(f"  ✓ Database {self.remote_db} already exists")
                return True
            else:
                self.log_progress(f"  → Database {self.remote_db} does not exist, creating it...")
                
                # Create the database
                create_cmd = (f"export PGPASSWORD='{self.remote_db_password}' && "
                             f"psql -h {self.remote_host} -p {self.remote_port} "
                             f"-U {self.remote_db_user} -d postgres "
                             f"-c \"CREATE DATABASE {self.remote_db};\"")
                
                self.log_progress(f"  → Creating: psql -h {self.remote_host} -p {self.remote_port} -U {self.remote_db_user} -d postgres -c \"CREATE DATABASE {self.remote_db};\"")
                
                _, stdout, stderr = self.ssh.exec_command(create_cmd)
                exit_status = stdout.channel.recv_exit_status()
                
                if exit_status == 0:
                    self.log_progress(f"  ✓ Database {self.remote_db} created successfully")
                    return True
                else:
                    error_msg = stderr.read().decode().strip()
                    safe_error = error_msg.replace(self.remote_db_password, '[PROTECTED]') if self.remote_db_password else error_msg
                    self.log_progress(f"  ✗ Failed to create database: {safe_error}")
                    return False
                
        except Exception as e:
            safe_error = str(e).replace(self.remote_db_password, '[PROTECTED]') if self.remote_db_password else str(e)
            self.log_progress(f"  ✗ Database creation check error: {safe_error}")
            return False

    def terminate_active_connections(self):
        """Terminate active connections to the target database before import"""
        self.log_progress(f"Terminating active connections to database {self.remote_db}...")
        try:
            # Connect to postgres database to terminate connections to target database
            terminate_cmd = (f"export PGPASSWORD='{self.remote_db_password}' && "
                           f"psql -h {self.remote_host} -p {self.remote_port} "
                           f"-U {self.remote_db_user} -d postgres "
                           f"-c \"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity "
                           f"WHERE pg_stat_activity.datname = '{self.remote_db}' AND pid <> pg_backend_pid();\"")
            
            self.log_progress(f"  → Terminating connections to {self.remote_db}")
            
            _, stdout, stderr = self.ssh.exec_command(terminate_cmd)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status == 0:
                stdout.read().decode().strip()  # Clear stdout buffer
                self.log_progress(f"  ✓ Active connections terminated")
                return True
            else:
                error_msg = stderr.read().decode().strip()
                safe_error = error_msg.replace(self.remote_db_password, '[PROTECTED]') if self.remote_db_password else error_msg
                self.log_progress(f"  ⚠ Connection termination warning: {safe_error}")
                # Don't fail here - the connections might not exist or we might not have permission
                return True
                
        except Exception as e:
            safe_error = str(e).replace(self.remote_db_password, '[PROTECTED]') if self.remote_db_password else str(e)
            self.log_progress(f"  ⚠ Connection termination warning: {safe_error}")
            return True  # Don't fail the process for this

    def test_remote_db_connection(self):
        """Test connection to remote PostgreSQL database before importing"""
        self.log_progress(f"Testing connection to remote database {self.remote_host}:{self.remote_port}/{self.remote_db}...")
        try:
            test_cmd = (f"export PGPASSWORD='{self.remote_db_password}' && "
                       f"psql -h {self.remote_host} -p {self.remote_port} "
                       f"-U {self.remote_db_user} -d {self.remote_db} -c 'SELECT version();' -t")
            
            self.log_progress(f"  → Testing: psql -h {self.remote_host} -p {self.remote_port} -U {self.remote_db_user} -d {self.remote_db} -c 'SELECT version();'")
            
            _, stdout, stderr = self.ssh.exec_command(test_cmd)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status == 0:
                version_info = stdout.read().decode().strip()
                self.log_progress(f"  ✓ Database connection successful")
                self.log_progress(f"  → PostgreSQL version: {version_info}")
                return True
            else:
                error_msg = stderr.read().decode().strip()
                safe_error = error_msg.replace(self.remote_db_password, '[PROTECTED]') if self.remote_db_password else error_msg
                self.log_progress(f"  ✗ Database connection failed: {safe_error}")
                return False
                
        except Exception as e:
            safe_error = str(e).replace(self.remote_db_password, '[PROTECTED]') if self.remote_db_password else str(e)
            self.log_progress(f"  ✗ Connection test error: {safe_error}")
            return False
    
    def import_to_remote_db(self):
        self.log_progress(f"Starting import to remote PostgreSQL database ({self.remote_host}:{self.remote_port}/{self.remote_db})...")
        try:
            # Create database if it doesn't exist
            if not self.create_remote_db_if_not_exists():
                self.log_progress("  ✗ Aborting import due to database creation failure")
                return False
            
            # Terminate active connections to prevent import conflicts
            if not self.terminate_active_connections():
                self.log_progress("  ⚠ Could not terminate connections, continuing anyway...")
            
            # Test database connection first
            if not self.test_remote_db_connection():
                self.log_progress("  ✗ Aborting import due to connection failure")
                return False
            
            for i, remote_file in enumerate(self.remote_files, 1):
                self.log_progress(f"[{i}/{len(self.remote_files)}] Processing {remote_file}...")
                
                # Step 1: Verify file exists and test decompression
                self.log_progress(f"  → Verifying file integrity...")
                
                # Check if file exists
                check_cmd = f"test -f {remote_file} && echo 'exists' || echo 'missing'"
                _, stdout, stderr = self.ssh.exec_command(check_cmd)
                file_status = stdout.read().decode().strip()
                
                if file_status != 'exists':
                    self.log_progress(f"  ✗ File not found: {remote_file}")
                    return False
                
                # Test decompression
                decompress_test_cmd = f"gunzip -t {remote_file}"
                _, stdout, stderr = self.ssh.exec_command(decompress_test_cmd)
                exit_status = stdout.channel.recv_exit_status()
                
                if exit_status != 0:
                    error_msg = stderr.read().decode().strip()
                    self.log_progress(f"  ✗ File integrity check failed: {error_msg}")
                    return False
                
                self.log_progress(f"  ✓ File integrity verified")
                
                # Step 2: Get file size for progress tracking
                size_cmd = f"du -h {remote_file} | cut -f1"
                _, stdout, stderr = self.ssh.exec_command(size_cmd)
                file_size = stdout.read().decode().strip()
                self.log_progress(f"  → File size: {file_size}")
                
                # Step 3: Import to database with enhanced error handling and progress monitoring
                self.log_progress(f"  → Importing to PostgreSQL database...")
                
                # Create a more robust import command with verbose output
                import_cmd = (f"export PGPASSWORD='{self.remote_db_password}' && "
                            f"gunzip -c {remote_file} | "
                            f"psql -h {self.remote_host} -p {self.remote_port} "
                            f"-U {self.remote_db_user} -d {self.remote_db} "
                            f"-v ON_ERROR_STOP=1 --echo-errors")
                
                self.log_progress(f"  → Executing: export PGPASSWORD=[PROTECTED] && gunzip -c {remote_file} | psql -h {self.remote_host} -p {self.remote_port} -U {self.remote_db_user} -d {self.remote_db} -v ON_ERROR_STOP=1 --echo-errors")
                
                # Execute command and monitor progress
                _, stdout, stderr = self.ssh.exec_command(import_cmd, timeout=7200)  # 2 hour timeout
                
                # Monitor progress by checking database size periodically
                import threading
                import time
                
                def monitor_progress():
                    """Monitor database size during import"""
                    previous_size = 0
                    check_count = 0
                    
                    while not stdout.channel.exit_status_ready():
                        try:
                            time.sleep(30)  # Check every 30 seconds
                            check_count += 1
                            
                            # Get database size
                            size_cmd = (f"export PGPASSWORD='{self.remote_db_password}' && "
                                      f"psql -h {self.remote_host} -p {self.remote_port} "
                                      f"-U {self.remote_db_user} -d {self.remote_db} "
                                      f"-t -c \"SELECT pg_size_pretty(pg_database_size('{self.remote_db}'));\"")
                            
                            _, size_stdout, _ = self.ssh.exec_command(size_cmd)
                            current_size = size_stdout.read().decode().strip()
                            
                            if current_size and current_size != previous_size:
                                self.log_progress(f"    → Import progress: Database size is now {current_size} (check #{check_count})")
                                previous_size = current_size
                            elif check_count % 4 == 0:  # Every 2 minutes, show we're still alive
                                self.log_progress(f"    → Import still running... (check #{check_count}, size: {current_size or 'checking...'})")
                                
                        except Exception as e:
                            # Don't let monitoring errors break the import
                            if check_count % 10 == 0:  # Only log every 5 minutes
                                self.log_progress(f"    → Import monitoring note: {str(e)[:100]}")
                            pass
                
                # Start progress monitoring in background
                monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
                monitor_thread.start()
                
                # Wait for completion and collect output
                exit_status = stdout.channel.recv_exit_status()
                stdout_content = stdout.read().decode().strip()
                stderr_content = stderr.read().decode().strip()
                
                if exit_status == 0:
                    self.log_progress(f"  ✓ Import completed successfully")
                    if stdout_content and stdout_content != '':
                        self.log_progress(f"    → Output: {stdout_content}")
                else:
                    self.log_progress(f"  ✗ Import failed (exit code: {exit_status})")
                    if stderr_content:
                        # Filter out sensitive information from error messages
                        safe_error = stderr_content.replace(self.remote_db_password, '[PROTECTED]')
                        # Show only the first few lines of error to avoid log spam
                        error_lines = safe_error.split('\n')[:10]
                        for line in error_lines:
                            if line.strip():
                                self.log_progress(f"    → Error: {line.strip()}")
                    return False
            
            self.log_progress("✓ All database imports completed successfully")
            return True
            
        except Exception as e:
            # Ensure password is not exposed in exception logs
            safe_error = str(e).replace(self.remote_db_password, '[PROTECTED]') if self.remote_db_password else str(e)
            self.log_progress(f"✗ Remote import failed: {safe_error}")
            return False
    
    def cleanup_remote_files(self):
        self.log_progress("Cleaning up remote dump files...")
        try:
            for remote_file in getattr(self, 'remote_files', []):
                try:
                    self.sftp.remove(remote_file)
                    self.log_progress(f"✓ Removed {remote_file}")
                except Exception as e:
                    self.log_progress(f"⚠ Could not remove {remote_file}: {e}")
            
            return True
            
        except Exception as e:
            self.log_progress(f"⚠ Remote cleanup warning: {e}")
            return True  # Don't fail the process for cleanup issues
    
    def cleanup_local_files(self):
        self.log_progress("Cleaning up local temporary files...")
        try:
            if hasattr(self, 'sftp'):
                self.sftp.close()
            if hasattr(self, 'ssh'):
                self.ssh.close()
            
            if self.tmp_dir.exists():
                shutil.rmtree(self.tmp_dir)
            
            self.log_progress("✓ Local cleanup completed")
        except Exception as e:
            self.log_progress(f"⚠ Local cleanup warning: {e}")
    
    def refresh_database(self):
        self.log_progress("🚀 Starting PostgreSQL database refresh process...")
        
        try:
            self.setup_tmp_directory()
            
            if not self.create_local_dump():
                return False
            
            if not self.split_dump_if_needed():
                return False
            
            if not self.compress_dumps():
                return False
            
            if not self.connect_to_server():
                return False
            
            if not self.upload_dumps():
                return False
            
            if not self.import_to_remote_db():
                self.log_progress("✗ Database import failed, cleaning up...")
                self.cleanup_remote_files()
                return False
            
            if not self.cleanup_remote_files():
                return False
            
            self.log_progress("🎉 PostgreSQL database refresh completed successfully!")
            self.cleanup_local_files()
            
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
        manager = PostgresRefreshManager()
        success = manager.refresh_database()
        exit(0 if success else 1)
    except Exception as e:
        print(f"Fatal error: {e}")
        exit(1)

if __name__ == "__main__":
    main()