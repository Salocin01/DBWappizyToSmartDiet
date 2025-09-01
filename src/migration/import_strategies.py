from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Any
from bson import ObjectId
import psycopg2
from datetime import datetime, time


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
        
        conn.rollback()
        successful_count = 0
        
        for values in batch_values:
            try:
                cursor.execute(sql, values)
                summary.record_success(table_name)
                successful_count += 1
            except psycopg2.IntegrityError as individual_e:
                error_message = str(individual_e).lower()
                failed_record = {'id': values[0] if values else 'unknown', 'values': values}
                
                if "foreign key constraint" in error_message:
                    summary.record_error(table_name, 'Foreign key constraint', failed_record)
                elif "null value" in error_message or "not-null constraint" in error_message:
                    summary.record_error(table_name, 'NULL constraint', failed_record)
                else:
                    summary.record_error(table_name, f'Other integrity error: {str(individual_e)[:100]}', failed_record)
                conn.rollback()
                continue
        
        conn.commit()
        return successful_count
    
    @staticmethod
    def execute_batch(conn, batch_values, columns, table_name, summary_instance, use_on_conflict=False):
        """Execute a batch insert with error handling and progress tracking"""
        from .import_summary import ImportSummary
        
        if not batch_values or not columns:
            return 0
            
        summary = summary_instance or ImportSummary()
        cursor = conn.cursor()
        
        placeholders = ', '.join(['%s'] * len(columns))
        on_conflict_clause = " ON CONFLICT (id) DO NOTHING" if use_on_conflict else ""
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}){on_conflict_clause}"
        
        try:
            cursor.executemany(sql, batch_values)
            actual_insertions = cursor.rowcount
            skipped_count = len(batch_values) - actual_insertions
            
            summary.record_success(table_name, actual_insertions)
            if skipped_count > 0:
                summary.record_skipped(table_name, skipped_count)
            
            conn.commit()
            return actual_insertions
            
        except psycopg2.IntegrityError:
            return ImportUtils.handle_batch_errors(
                conn, cursor, sql, batch_values, table_name, summary_instance
            )


class ImportStrategy(ABC):
    @abstractmethod
    def export_data(self, conn, collection, config: ImportConfig):
        pass
    
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
    
    def export_data(self, conn, collection, config: ImportConfig):
        from src.schemas.schemas import TABLE_SCHEMAS
        from .import_summary import ImportSummary
        
        schema = TABLE_SCHEMAS[config.table_name]
        summary = config.summary_instance or ImportSummary()
        cursor = conn.cursor()
        
        # Get total count for progress tracking
        total_docs = self.count_total_documents(collection, config)
        processed_docs = 0
        
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
                    batch_values.append(values)
            
            if batch_values:
                actual_insertions = ImportUtils.execute_batch(
                    conn, batch_values, columns, config.table_name, config.summary_instance
                )
                processed_docs += actual_insertions
                skipped_count = len(batch_values) - actual_insertions
                print(f"Processed {processed_docs}/{total_docs} documents for {config.table_name} (tried {len(batch_values)}, inserted {actual_insertions}, skipped {skipped_count})")
            
            offset += config.batch_size
            
            if len(documents) < config.batch_size:
                break
        
        print(f"Completed processing {processed_docs} documents for {config.table_name}")
        cursor.close()


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
        child_ids = document.get(self.config.array_field, [])
        
        if not child_ids:
            return [], self.config.sql_columns
        
        # Fetch all child documents for this parent
        children_docs = {}
        if child_collection is not None:
            child_cursor = child_collection.find(
                {'_id': {'$in': child_ids}},
                self.config.child_projection_fields
            )
            for child_doc in child_cursor:
                children_docs[child_doc['_id']] = child_doc
        
        # Build values for all children of this parent
        batch_values = []
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
        
        return batch_values, self.config.sql_columns
    
    def export_data(self, conn, collection, config: ImportConfig):
        from src.connections.mongo_connection import get_mongo_collection
        
        cursor = conn.cursor()
        
        child_collection = get_mongo_collection(self.config.child_collection) if self.config.child_collection else collection
        
        total_parents = self.count_total_documents(collection, config)
        processed_parents = 0
        total_children = 0
        
        offset = 0
        while True:
            parents = self.get_documents(collection, config, offset)
            
            if not parents:
                break
            
            batch_values = []
            columns = None
            
            for parent in parents:
                parent_batch_values, doc_columns = self.extract_data_for_sql(parent, config)
                if parent_batch_values:
                    if columns is None:
                        columns = doc_columns
                    batch_values.extend(parent_batch_values)
            
            if batch_values:
                actual_insertions = ImportUtils.execute_batch(
                    conn, batch_values, columns, config.table_name, config.summary_instance, use_on_conflict=True
                )
                total_children += actual_insertions
            
            processed_parents += len(parents)
            print(f"Processed {processed_parents}/{total_parents} {self.config.parent_collection}, {total_children} {config.table_name}")
            
            offset += config.batch_size
            
            if len(parents) < config.batch_size:
                break
        
        print(f"Completed processing {total_children} records from {processed_parents} {self.config.parent_collection}")
        cursor.close()
    
    def _default_transform(self, parent_id, child_doc):
        """Default transformation - override with custom transformer if needed"""
        return [
            str(child_doc['_id']),
            parent_id,
            child_doc.get('creation_date'),
            child_doc.get('update_date')
        ]