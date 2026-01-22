"""
Matomo Analytics Data Synchronization

This module synchronizes Matomo analytics tables from MariaDB to PostgreSQL.
It handles incremental updates based on timestamps to efficiently sync only new/updated data.

Supported tables:
- matomo_log_visit: Visit session data
- matomo_log_link_visit_action: Page view and action tracking data
"""

import yaml
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
import pymysql
import psycopg2
from psycopg2.extras import execute_batch


class MatomoTableSchema:
    """Represents a Matomo table schema for migration"""

    def __init__(self, table_name: str, config: Dict[str, Any]):
        self.table_name = table_name
        self.description = config.get('description', '')
        self.source_table = config.get('source_table', table_name)
        self.columns = config.get('columns', [])

    def get_create_sql(self) -> str:
        """Generate PostgreSQL CREATE TABLE statement"""
        column_definitions = []

        for col in self.columns:
            col_def = f"{col['name']} {col['sql_type']}"
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            if col.get('primary_key', False):
                col_def += " PRIMARY KEY"
            column_definitions.append(col_def)

        return f"CREATE TABLE IF NOT EXISTS {self.table_name} (\n  {',\n  '.join(column_definitions)}\n)"

    def get_column_names(self) -> List[str]:
        """Get list of column names"""
        return [col['name'] for col in self.columns]


def load_matomo_schemas() -> Dict[str, MatomoTableSchema]:
    """Load Matomo table schemas from YAML configuration"""
    config_path = os.path.join(os.path.dirname(__file__), '../../config/matomo_schemas.yaml')

    with open(config_path, 'r') as f:
        schemas_config = yaml.safe_load(f)

    schemas = {}
    for table_name, config in schemas_config.items():
        schemas[table_name] = MatomoTableSchema(table_name, config)

    return schemas


def get_last_sync_timestamp(pg_conn, table_name: str, timestamp_column: str = 'visit_last_action_time') -> Optional[datetime]:
    """Get the last synchronized timestamp from PostgreSQL table"""
    cursor = pg_conn.cursor()
    try:
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )
        """, (table_name,))

        if not cursor.fetchone()[0]:
            return None

        # Get max timestamp from the specified column
        cursor.execute(f"SELECT MAX({timestamp_column}) FROM {table_name}")
        result = cursor.fetchone()
        return result[0] if result and result[0] else None
    except psycopg2.Error as e:
        print(f"   ⚠️  Error getting last sync timestamp: {e}")
        return None
    finally:
        cursor.close()


def convert_mariadb_value(value: Any, sql_type: str) -> Any:
    """Convert MariaDB value to PostgreSQL-compatible format"""
    if value is None:
        return None

    # Convert binary data to bytea format
    if sql_type == 'BYTEA':
        if isinstance(value, bytes):
            return psycopg2.Binary(value)
        return value

    # Convert datetime
    if sql_type == 'TIMESTAMP' and isinstance(value, datetime):
        return value

    return value


def sync_matomo_table(maria_conn, pg_conn, schema: MatomoTableSchema, after_date: Optional[datetime] = None):
    """Sync a single Matomo table from MariaDB to PostgreSQL"""
    print(f"\n{'='*80}")
    print(f"Syncing table: {schema.table_name}")
    print(f"Description: {schema.description}")
    print(f"{'='*80}")

    maria_cursor = maria_conn.cursor()
    pg_cursor = pg_conn.cursor()

    try:
        # Build query with optional date filter
        columns = schema.get_column_names()
        select_sql = f"SELECT {', '.join(columns)} FROM {schema.source_table}"

        # Determine timestamp column for filtering
        timestamp_col = None
        if schema.table_name == 'matomo_log_visit':
            timestamp_col = 'visit_last_action_time'
        elif schema.table_name == 'matomo_log_link_visit_action':
            timestamp_col = 'server_time'
        # matomo_log_action has no timestamp column - will do full comparison

        if after_date and timestamp_col:
            select_sql += f" WHERE {timestamp_col} > %s"
            maria_cursor.execute(select_sql, (after_date,))
            print(f"📅 Incremental sync from: {after_date}")
        else:
            maria_cursor.execute(select_sql)
            print(f"📅 Full sync (no previous data)")

        # Count total rows
        total_rows = maria_cursor.rowcount
        print(f"📊 Found {total_rows:,} rows to sync")

        if total_rows == 0:
            print("✅ No new data to sync")
            return

        # Prepare INSERT statement with ON CONFLICT DO UPDATE
        primary_key = next((col['name'] for col in schema.columns if col.get('primary_key')), None)

        insert_columns = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        if primary_key:
            update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key])
            insert_sql = f"""
                INSERT INTO {schema.table_name} ({insert_columns})
                VALUES ({placeholders})
                ON CONFLICT ({primary_key}) DO UPDATE SET {update_set}
            """
        else:
            insert_sql = f"INSERT INTO {schema.table_name} ({insert_columns}) VALUES ({placeholders})"

        # Fetch and insert in batches
        batch_size = 5000
        batch = []
        inserted_count = 0
        updated_count = 0
        error_count = 0

        for row in maria_cursor:
            # Convert row dict to tuple in correct column order
            if isinstance(row, dict):
                row_tuple = tuple(convert_mariadb_value(row[col],
                                  next(c['sql_type'] for c in schema.columns if c['name'] == col))
                                  for col in columns)
            else:
                row_tuple = tuple(convert_mariadb_value(val, schema.columns[i]['sql_type'])
                                  for i, val in enumerate(row))

            batch.append(row_tuple)

            if len(batch) >= batch_size:
                try:
                    execute_batch(pg_cursor, insert_sql, batch)
                    pg_conn.commit()
                    inserted_count += len(batch)
                    print(f"   ✓ Synced {inserted_count:,} / {total_rows:,} rows", end='\r')
                except psycopg2.Error as e:
                    print(f"\n   ⚠️  Batch error: {e}")
                    pg_conn.rollback()

                    # Retry individually
                    for values in batch:
                        try:
                            pg_cursor.execute(insert_sql, values)
                            pg_conn.commit()
                            inserted_count += 1
                        except psycopg2.Error as e2:
                            error_count += 1
                            if error_count <= 5:  # Only print first 5 errors
                                print(f"   ⚠️  Row error: {e2}")

                batch = []

        # Insert remaining rows
        if batch:
            try:
                execute_batch(pg_cursor, insert_sql, batch)
                pg_conn.commit()
                inserted_count += len(batch)
            except psycopg2.Error as e:
                print(f"\n   ⚠️  Final batch error: {e}")
                pg_conn.rollback()

                for values in batch:
                    try:
                        pg_cursor.execute(insert_sql, values)
                        pg_conn.commit()
                        inserted_count += 1
                    except psycopg2.Error as e2:
                        error_count += 1
                        if error_count <= 5:
                            print(f"   ⚠️  Row error: {e2}")

        print(f"\n✅ Sync complete: {inserted_count:,} rows synced, {error_count} errors")

    except Exception as e:
        print(f"❌ Error syncing table {schema.table_name}: {e}")
        pg_conn.rollback()
        raise
    finally:
        maria_cursor.close()
        pg_cursor.close()


def setup_matomo_tables(pg_conn, schemas: Dict[str, MatomoTableSchema]):
    """Create or update Matomo tables in PostgreSQL"""
    print("\n📦 Setting up Matomo tables in PostgreSQL...")

    cursor = pg_conn.cursor()

    for table_name, schema in schemas.items():
        try:
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s
                )
            """, (table_name,))

            table_exists = cursor.fetchone()[0]

            if not table_exists:
                cursor.execute(schema.get_create_sql())
                pg_conn.commit()
                print(f"✅ Table {table_name} created")
            else:
                print(f"✅ Table {table_name} already exists")

        except psycopg2.Error as e:
            print(f"❌ Error setting up table {table_name}: {e}")
            pg_conn.rollback()

    cursor.close()


def run_matomo_sync():
    """Main function to run Matomo data synchronization"""
    from src.connections.mariadb_connection import get_mariadb_connection, close_mariadb_connection
    from src.connections.postgres_connection import connect_postgres, close_postgres_connection

    maria_conn = None
    pg_conn = None

    try:
        # Load schemas
        print("📋 Loading Matomo table schemas...")
        schemas = load_matomo_schemas()
        print(f"   → Loaded {len(schemas)} table schema(s)")

        # Connect to databases
        print("\n🔌 Connecting to databases...")
        maria_conn = get_mariadb_connection()
        print("   ✓ MariaDB connected")

        pg_conn = connect_postgres()
        print("   ✓ PostgreSQL connected")

        # Setup tables
        setup_matomo_tables(pg_conn, schemas)

        # Sync each table
        for table_name, schema in schemas.items():
            # Determine timestamp column for last sync check
            if table_name == 'matomo_log_visit':
                timestamp_col = 'visit_last_action_time'
            elif table_name == 'matomo_log_link_visit_action':
                timestamp_col = 'server_time'
            else:
                # Tables without timestamps (like matomo_log_action) will do full sync
                timestamp_col = None

            # Get last sync timestamp
            after_date = None
            if timestamp_col:
                after_date = get_last_sync_timestamp(pg_conn, table_name, timestamp_col)

            # Sync table
            sync_matomo_table(maria_conn, pg_conn, schema, after_date)

        print("\n" + "="*80)
        print("✅ Matomo data synchronization completed successfully!")
        print("="*80)

    except Exception as e:
        print(f"\n❌ Matomo sync failed: {e}")
        raise

    finally:
        # Close connections
        if maria_conn:
            maria_conn.close()
        if pg_conn:
            pg_conn.close()

        close_mariadb_connection()
        close_postgres_connection()


if __name__ == '__main__':
    run_matomo_sync()
