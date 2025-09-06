from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Any
from bson import ObjectId
import psycopg2
from datetime import datetime, time

# Control whether to use batch processing (True) or single-line SQL statements (False)
IMPORT_BY_BATCH = True
# Control whether to execute SQL directly (True) or generate SQL files (False)
DIRECT_IMPORT = True


@dataclass
class ImportConfig:
    table_name: str
    source_collection: str
    batch_size: int = 5000
    after_date: Optional[Any] = None
    custom_filter: Optional[Callable] = None
    summary_instance: Optional[Any] = None


class ImportUtils:
    @staticmethod
    def build_date_filter(after_date):
        """Build MongoDB date filter for incremental imports"""
        if not after_date:
            return {}
        
        if hasattr(after_date, 'date'):  # It's already a datetime
            return {'creation_date': {'$gte': after_date}}
        else:  # It's a date, convert to datetime
            return {'creation_date': {'$gte': datetime.combine(after_date, time.min)}}
    
    @staticmethod
    def handle_batch_errors(conn, cursor, sql, batch_values, table_name, summary_instance):
        """Handle batch insert errors with fallback to individual inserts"""
        from .import_summary import ImportSummary
        summary = summary_instance or ImportSummary()
        
        # Close the old cursor and get a fresh one after rollback
        cursor.close()
        cursor = conn.cursor()
        
        successful_count = 0
        
        for values in batch_values:
            cursor.execute("SAVEPOINT individual_retry")
            try:
                cursor.execute(sql, values)
                cursor.execute("RELEASE SAVEPOINT individual_retry")
                summary.record_success(table_name)
                successful_count += 1
            except psycopg2.IntegrityError as individual_e:
                cursor.execute("ROLLBACK TO SAVEPOINT individual_retry")
                error_message = str(individual_e).lower()
                failed_record = {'id': values[0] if values else 'unknown', 'values': values}
                
                if "foreign key constraint" in error_message:
                    summary.record_error(table_name, 'Foreign key constraint', failed_record)
                elif "null value" in error_message or "not-null constraint" in error_message:
                    summary.record_error(table_name, 'NULL constraint', failed_record)
                else:
                    summary.record_error(table_name, f'Other integrity error: {str(individual_e)[:100]}', failed_record)
                continue
            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT individual_retry")
                failed_record = {'id': values[0] if values else 'unknown', 'values': values}
                summary.record_error(table_name, f'Unexpected error: {str(e)[:100]}', failed_record)
                continue
        
        conn.commit()
        cursor.close()
        return successful_count
    
    @staticmethod
    def execute_sql_file(conn, sql_file_path, summary_instance):
        """Execute SQL file against the database"""
        from .import_summary import ImportSummary
        import os
        
        summary = summary_instance or ImportSummary()
        
        if not os.path.exists(sql_file_path):
            return 0
            
        cursor = conn.cursor()
        executed_count = 0
        failed_count = 0
        
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
                
            # Split by semicolons and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for i, statement in enumerate(statements):
                cursor.execute("SAVEPOINT sql_statement")
                try:
                    cursor.execute(statement)
                    cursor.execute("RELEASE SAVEPOINT sql_statement")
                    executed_count += 1
                except psycopg2.IntegrityError as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT sql_statement")
                    failed_count += 1
                    # Extract table name from INSERT statement for better tracking
                    table_name = "unknown"
                    if "INSERT INTO" in statement.upper():
                        try:
                            table_name = statement.upper().split("INSERT INTO")[1].split()[0]
                        except:
                            pass
                    summary.record_error(table_name, f'SQL file integrity error: {str(e)[:100]}', {'statement_index': i})
                    continue
                except Exception as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT sql_statement")
                    failed_count += 1
                    table_name = "unknown"
                    if "INSERT INTO" in statement.upper():
                        try:
                            table_name = statement.upper().split("INSERT INTO")[1].split()[0]
                        except:
                            pass
                    summary.record_error(table_name, f'SQL file execution error: {str(e)[:100]}', {'statement_index': i})
                    print(f"Error executing SQL statement {i+1}: {e}")
                    continue
            
            conn.commit()
            print(f"SQL file execution completed: {executed_count} successful, {failed_count} failed")
            return executed_count
            
        except Exception as e:
            print(f"Error reading SQL file {sql_file_path}: {e}")
            conn.rollback()
            return 0
        finally:
            cursor.close()

    @staticmethod
    def write_sql_file(batch_values, columns, table_name, summary_instance, use_on_conflict=False, on_conflict_clause=None):
        """Generate SQL file for batch values"""
        from .import_summary import ImportSummary
        import os
        
        if not batch_values or not columns:
            return 0
            
        summary = summary_instance or ImportSummary()
        
        if on_conflict_clause:
            conflict_clause = on_conflict_clause
        else:
            conflict_clause = " ON CONFLICT (id) DO NOTHING" if use_on_conflict else ""
        
        # Generate SQL file
        os.makedirs("sql_exports", exist_ok=True)
        sql_file_path = f"sql_exports/{table_name}_import.sql"
        
        with open(sql_file_path, 'a', encoding='utf-8') as f:
            for values in batch_values:
                # Format values for SQL file
                formatted_values = []
                for value in values:
                    if value is None:
                        formatted_values.append('NULL')
                    elif isinstance(value, str):
                        # Escape single quotes in strings
                        escaped_value = value.replace("'", "''")
                        formatted_values.append(f"'{escaped_value}'")
                    elif isinstance(value, datetime):
                        formatted_values.append(f"'{value.isoformat()}'")
                    else:
                        formatted_values.append(str(value))
                
                sql_statement = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(formatted_values)}){conflict_clause};\n"
                f.write(sql_statement)
        
        # Record as successful for tracking purposes
        summary.record_success(table_name, len(batch_values))
        print(f"Generated SQL for {len(batch_values)} records in {sql_file_path}")
        return len(batch_values)
    
    @staticmethod
    def execute_direct_sql(conn, batch_values, columns, table_name, summary_instance, use_on_conflict=False, on_conflict_clause=None):
        """Execute SQL queries directly on the database"""
        from .import_summary import ImportSummary
        
        if not batch_values or not columns:
            return 0
            
        summary = summary_instance or ImportSummary()
        
        placeholders = ', '.join(['%s'] * len(columns))
        if on_conflict_clause:
            conflict_clause = on_conflict_clause
        else:
            conflict_clause = " ON CONFLICT (id) DO NOTHING" if use_on_conflict else ""
        sql_template = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}){conflict_clause}"
        
        cursor = conn.cursor()
        
        try:
            if IMPORT_BY_BATCH:
                # Use transaction savepoint for batch processing
                cursor.execute("SAVEPOINT batch_insert")
                try:
                    cursor.executemany(sql_template, batch_values)
                    actual_insertions = cursor.rowcount
                    
                    # Verify actual insertions by counting rows
                    if not use_on_conflict:
                        # For non-conflict queries, rowcount should match batch size
                        if actual_insertions != len(batch_values):
                            cursor.execute("ROLLBACK TO SAVEPOINT batch_insert")
                            # Fall back to individual processing
                            return ImportUtils.handle_batch_errors(
                                conn, cursor, sql_template, batch_values, table_name, summary_instance
                            )
                    
                    cursor.execute("RELEASE SAVEPOINT batch_insert")
                    skipped_count = len(batch_values) - actual_insertions
                    
                    summary.record_success(table_name, actual_insertions)
                    if skipped_count > 0:
                        summary.record_skipped(table_name, skipped_count)
                    
                    conn.commit()
                    return actual_insertions
                    
                except psycopg2.IntegrityError:
                    cursor.execute("ROLLBACK TO SAVEPOINT batch_insert")
                    # Fall back to individual processing
                    return ImportUtils.handle_batch_errors(
                        conn, cursor, sql_template, batch_values, table_name, summary_instance
                    )
            else:
                # Use individual transactions with savepoints
                successful_count = 0
                for values in batch_values:
                    cursor.execute("SAVEPOINT individual_insert")
                    try:
                        cursor.execute(sql_template, values)
                        cursor.execute("RELEASE SAVEPOINT individual_insert")
                        summary.record_success(table_name)
                        successful_count += 1
                    except psycopg2.IntegrityError as e:
                        cursor.execute("ROLLBACK TO SAVEPOINT individual_insert")
                        error_message = str(e).lower()
                        failed_record = {'id': values[0] if values else 'unknown', 'values': values}
                        
                        if "foreign key constraint" in error_message:
                            summary.record_error(table_name, 'Foreign key constraint', failed_record)
                        elif "null value" in error_message or "not-null constraint" in error_message:
                            summary.record_error(table_name, 'NULL constraint', failed_record)
                        else:
                            summary.record_error(table_name, f'Other integrity error: {str(e)[:100]}', failed_record)
                        continue
                
                conn.commit()
                return successful_count
            
        except Exception as e:
            conn.rollback()
            cursor.close()
            cursor = conn.cursor()  # Get fresh cursor
            raise e
        finally:
            cursor.close()
    
    @staticmethod
    def execute_batch(conn, batch_values, columns, table_name, summary_instance, use_on_conflict=False, on_conflict_clause=None):
        """Execute insert with error handling and progress tracking or generate SQL files"""
        if DIRECT_IMPORT:
            return ImportUtils.execute_direct_sql(
                conn, batch_values, columns, table_name, summary_instance, use_on_conflict, on_conflict_clause
            )
        else:
            # Generate SQL file (accumulate without executing)
            return ImportUtils.write_sql_file(
                batch_values, columns, table_name, summary_instance, use_on_conflict, on_conflict_clause
            )


class ImportStrategy(ABC):
    @abstractmethod
    def count_total_documents(self, collection, config: ImportConfig) -> int:
        """Count total documents that will be processed"""
        pass
    
    @abstractmethod
    def get_documents(self, collection, config: ImportConfig, offset: int = 0):
        """Get documents for processing with pagination"""
        pass
    
    @abstractmethod
    def extract_data_for_sql(self, document, config: ImportConfig):
        """Extract and prepare data from a single document for SQL insertion"""
        pass
    
    def get_use_on_conflict(self) -> bool:
        """Override in subclasses if ON CONFLICT clause is needed"""
        return False
    
    def get_on_conflict_clause(self, table_name: str) -> str:
        """Get the ON CONFLICT clause for the table. Override in subclasses if needed."""
        if not self.get_use_on_conflict():
            return ""
        
        # Try to get the table schema to determine the appropriate conflict clause
        try:
            from src.schemas.schemas import TABLE_SCHEMAS
            schema = TABLE_SCHEMAS.get(table_name)
            if schema:
                return schema.get_on_conflict_clause()
        except Exception:
            pass
        
        # Fallback to default behavior
        return " ON CONFLICT (id) DO NOTHING"
    
    def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
        """Override in subclasses for custom progress messages"""
        return f"Processed {processed}/{total} documents for {table_name}"
    
    def export_data(self, conn, collection, config: ImportConfig):
        """Generic export implementation that works for both strategies"""
        import os
        
        # Ensure SQL exports directory exists
        if not DIRECT_IMPORT:
            os.makedirs("sql_exports", exist_ok=True)
        
        if DIRECT_IMPORT:
            cursor = conn.cursor()
        
        # Get total count for progress tracking
        total_docs = self.count_total_documents(collection, config)
        processed_docs = 0
        total_records = 0
        
        # Process documents in batches
        offset = 0
        while True:
            documents = self.get_documents(collection, config, offset)
            
            if not documents:
                break
            
            batch_values = []
            columns = None
            
            for doc in documents:
                values, doc_columns = self.extract_data_for_sql(doc, config)
                if values is not None:
                    if columns is None:
                        columns = doc_columns
                    # Handle both single records and multiple records per document
                    if isinstance(values, list) and len(values) > 0 and isinstance(values[0], list):
                        batch_values.extend(values)
                    else:
                        batch_values.append(values)
            
            if batch_values:
                actual_insertions = ImportUtils.execute_batch(
                    conn, batch_values, columns, config.table_name, 
                    config.summary_instance, use_on_conflict=self.get_use_on_conflict(),
                    on_conflict_clause=self.get_on_conflict_clause(config.table_name)
                )
                total_records += actual_insertions
                
                if DIRECT_IMPORT:
                    skipped_count = len(batch_values) - actual_insertions
                    print(self.get_progress_message(
                        processed_docs + len(documents), total_docs, config.table_name,
                        tried=len(batch_values), inserted=actual_insertions, skipped=skipped_count,
                        total_records=total_records
                    ))
                else:
                    print(f"Generated SQL for {total_records} records from {processed_docs + len(documents)}/{total_docs} documents for {config.table_name}")
            
            processed_docs += len(documents)
            offset += config.batch_size
            
            if len(documents) < config.batch_size:
                break
        
        action = "processing" if DIRECT_IMPORT else "SQL generation for"
        print(f"Completed {action} {total_records} records from {processed_docs} documents for {config.table_name}")
        
        if DIRECT_IMPORT:
            cursor.close()
        else:
            # Execute the accumulated SQL file as one entity row
            sql_file_path = f"sql_exports/{config.table_name}_import.sql"
            if os.path.exists(sql_file_path):
                executed_count = ImportUtils.execute_sql_file(conn, sql_file_path, config.summary_instance)
                os.remove(sql_file_path)
                print(f"Executed and deleted SQL file: {sql_file_path} ({executed_count} statements)")
                return executed_count


class DirectTranslationStrategy(ImportStrategy):
    """Handles simple 1:1 collection-to-table imports using schema field mappings"""
    
    def count_total_documents(self, collection, config: ImportConfig) -> int:
        """Count total documents that will be processed"""
        mongo_filter = ImportUtils.build_date_filter(config.after_date)
        return collection.count_documents(mongo_filter)
    
    def get_documents(self, collection, config: ImportConfig, offset: int = 0):
        """Get documents for processing with pagination"""
        mongo_filter = ImportUtils.build_date_filter(config.after_date)
        return list(collection.find(mongo_filter).skip(offset).limit(config.batch_size))
    
    def extract_data_for_sql(self, document, config: ImportConfig):
        """Extract and prepare data from a single document for SQL insertion"""
        from src.schemas.schemas import TABLE_SCHEMAS
        
        schema = TABLE_SCHEMAS[config.table_name]
        
        if config.custom_filter and not config.custom_filter(document):
            return None, None
            
        values = []
        columns = []
        
        for mongo_field, pg_field in schema.field_mappings.items():
            columns.append(pg_field)
            
            if mongo_field == '_id':
                value = str(document['_id'])
            elif mongo_field in document:
                value = document[mongo_field]
                if isinstance(value, ObjectId):
                    value = str(value)
            else:
                # Handle missing fields by setting to None (NULL in PostgreSQL)
                value = None
                
            values.append(value)
        
        return values, columns
    
    def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
        tried = kwargs.get('tried', 0)
        inserted = kwargs.get('inserted', 0)
        skipped = kwargs.get('skipped', 0)
        return f"Processed {processed}/{total} documents for {table_name} (tried {tried}, inserted {inserted}, skipped {skipped})"


@dataclass
class ArrayExtractionConfig:
    parent_collection: str
    array_field: str
    child_collection: str = None
    parent_filter_fields: Dict[str, str] = None
    child_projection_fields: Dict[str, str] = None
    sql_columns: List[str] = None
    value_transformer: Optional[Callable] = None


class ArrayExtractionStrategy(ImportStrategy):
    """Handles complex array-based imports that extract from parent document arrays"""
    
    def __init__(self, extraction_config: ArrayExtractionConfig):
        self.config = extraction_config
    
    def count_total_documents(self, collection, config: ImportConfig) -> int:
        """Count total parent documents that will be processed"""
        from src.connections.mongo_connection import get_mongo_collection
        
        parent_collection = get_mongo_collection(self.config.parent_collection)
        parent_filter = {self.config.array_field: {'$exists': True, '$ne': []}}
        parent_filter.update(ImportUtils.build_date_filter(config.after_date))
        
        return parent_collection.count_documents(parent_filter)
    
    def get_documents(self, collection, config: ImportConfig, offset: int = 0):
        """Get parent documents for processing with pagination"""
        from src.connections.mongo_connection import get_mongo_collection
        
        parent_collection = get_mongo_collection(self.config.parent_collection)
        parent_filter = {self.config.array_field: {'$exists': True, '$ne': []}}
        parent_filter.update(ImportUtils.build_date_filter(config.after_date))
        
        return list(parent_collection.find(
            parent_filter,
            self.config.parent_filter_fields or {'_id': 1, self.config.array_field: 1}
        ).sort('creation_date', 1).skip(offset).limit(config.batch_size))
    
    def extract_data_for_sql(self, document, config: ImportConfig):
        """Extract and prepare data from a single parent document for SQL insertion"""
        from src.connections.mongo_connection import get_mongo_collection
        from .import_summary import ImportSummary
        
        summary = config.summary_instance or ImportSummary()
        child_collection = get_mongo_collection(self.config.child_collection) if self.config.child_collection else None
        
        parent_id = str(document['_id'])
        array_items = document.get(self.config.array_field, [])
        
        if not array_items:
            return [], self.config.sql_columns
        
        # Build values for all children of this parent
        batch_values = []
        
        if child_collection is not None:
            # Traditional case: array contains ObjectIds referencing separate documents
            # Check if array_items contains ObjectIds or embedded documents
            if array_items and isinstance(array_items[0], dict):
                # Array contains embedded documents, process them directly
                for child_doc in array_items:
                    if self.config.value_transformer:
                        values = self.config.value_transformer(parent_id, child_doc)
                    else:
                        values = self._default_transform(parent_id, child_doc)
                    batch_values.append(values)
            else:
                # Array contains ObjectIds
                child_ids = array_items
                children_docs = {}
                child_cursor = child_collection.find(
                    {'_id': {'$in': child_ids}},
                    self.config.child_projection_fields
                )
                for child_doc in child_cursor:
                    children_docs[child_doc['_id']] = child_doc
                
                for child_id in child_ids:
                    if child_id in children_docs:
                        child_doc = children_docs[child_id]
                        if self.config.value_transformer:
                            values = self.config.value_transformer(parent_id, child_doc)
                        else:
                            values = self._default_transform(parent_id, child_doc)
                        batch_values.append(values)
                    else:
                        failed_record = {'id': str(child_id), 'parent_id': parent_id}
                        summary.record_error(config.table_name, 'Child document not found', failed_record)
        else:
            # New case: array contains embedded documents directly
            for array_item in array_items:
                if self.config.value_transformer:
                    values = self.config.value_transformer(parent_id, array_item)
                else:
                    values = self._default_transform(parent_id, array_item)
                batch_values.append(values)
        
        return batch_values, self.config.sql_columns
    
    def get_use_on_conflict(self) -> bool:
        return True
    
    def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
        total_records = kwargs.get('total_records', 0)
        action = "Processed" if DIRECT_IMPORT else "Generated SQL for"
        return f"{action} {processed}/{total} {self.config.parent_collection}, {total_records} {table_name}"
    
    def _default_transform(self, parent_id, child_doc):
        """Default transformation - override with custom transformer if needed"""
        return [
            str(child_doc['_id']),
            parent_id,
            child_doc.get('creation_date'),
            child_doc.get('update_date')
        ]