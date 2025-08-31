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
            return {'creation_date': {'$gt': after_date}}
        else:  # It's a date, convert to datetime
            return {'creation_date': {'$gt': datetime.combine(after_date, time.min)}}
    
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
                if "foreign key constraint" in error_message:
                    summary.record_error(table_name, 'Foreign key constraint')
                elif "null value" in error_message or "not-null constraint" in error_message:
                    summary.record_error(table_name, 'NULL constraint')
                else:
                    raise individual_e
                conn.rollback()
                continue
        
        conn.commit()
        return successful_count


class ImportStrategy(ABC):
    @abstractmethod
    def export_data(self, conn, collection, config: ImportConfig):
        pass


class DirectTranslationStrategy(ImportStrategy):
    """Handles simple 1:1 collection-to-table imports using schema field mappings"""
    
    def export_data(self, conn, collection, config: ImportConfig):
        from src.schemas.schemas import TABLE_SCHEMAS
        from .import_summary import ImportSummary
        
        schema = TABLE_SCHEMAS[config.table_name]
        summary = config.summary_instance or ImportSummary()
        
        # Build MongoDB query filter
        mongo_filter = ImportUtils.build_date_filter(config.after_date)
        
        cursor = conn.cursor()
        
        # Get total count for progress tracking
        total_docs = collection.count_documents(mongo_filter)
        processed_docs = 0
        
        # Process documents in batches
        offset = 0
        while True:
            documents = list(collection.find(mongo_filter).skip(offset).limit(config.batch_size))
            
            if not documents:
                break
            
            batch_values = []
            columns = None
            
            for doc in documents:
                if config.custom_filter and not config.custom_filter(doc):
                    continue
                    
                values = []
                if columns is None:
                    columns = []
                    for mongo_field, pg_field in schema.field_mappings.items():
                        columns.append(pg_field)
                
                for mongo_field, pg_field in schema.field_mappings.items():
                    if mongo_field == '_id':
                        value = str(doc['_id'])
                    elif mongo_field in doc:
                        value = doc[mongo_field]
                        if isinstance(value, ObjectId):
                            value = str(value)
                    else:
                        value = None
                        
                    values.append(value)
                
                batch_values.append(values)
            
            if batch_values:
                placeholders = ', '.join(['%s'] * len(columns))
                sql = f"INSERT INTO {config.table_name} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"
                
                try:
                    cursor.executemany(sql, batch_values)
                    summary.record_success(config.table_name, len(batch_values))
                    processed_docs += len(batch_values)
                    conn.commit()
                    print(f"Processed {processed_docs}/{total_docs} documents for {config.table_name}")
                    
                except psycopg2.IntegrityError:
                    successful_count = ImportUtils.handle_batch_errors(
                        conn, cursor, sql, batch_values, config.table_name, config.summary_instance
                    )
                    processed_docs += successful_count
            
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
    
    def export_data(self, conn, collection, config: ImportConfig):
        from src.connections.mongo_connection import get_mongo_collection
        from .import_summary import ImportSummary
        
        cursor = conn.cursor()
        summary = config.summary_instance or ImportSummary()
        
        parent_collection = get_mongo_collection(self.config.parent_collection)
        child_collection = get_mongo_collection(self.config.child_collection) if self.config.child_collection else collection
        
        # Build parent query filter
        parent_filter = {self.config.array_field: {'$exists': True, '$ne': []}}
        parent_filter.update(ImportUtils.build_date_filter(config.after_date))
        
        total_parents = parent_collection.count_documents(parent_filter)
        processed_parents = 0
        total_children = 0
        
        offset = 0
        while True:
            parents = list(parent_collection.find(
                parent_filter,
                self.config.parent_filter_fields or {'_id': 1, self.config.array_field: 1}
            ).sort('creation_date', 1).skip(offset).limit(config.batch_size))
            
            if not parents:
                break
            
            # Collect all child IDs from this batch
            all_child_ids = []
            parent_to_children = {}
            
            for parent in parents:
                parent_id = str(parent['_id'])
                child_ids = parent.get(self.config.array_field, [])
                parent_to_children[parent_id] = child_ids
                all_child_ids.extend(child_ids)
            
            # Fetch all child documents in one query
            children_docs = {}
            if all_child_ids:
                child_cursor = child_collection.find(
                    {'_id': {'$in': all_child_ids}},
                    self.config.child_projection_fields
                )
                for child_doc in child_cursor:
                    children_docs[child_doc['_id']] = child_doc
            
            # Build batch values
            batch_values = []
            for parent_id, child_ids in parent_to_children.items():
                for child_id in child_ids:
                    if child_id in children_docs:
                        child_doc = children_docs[child_id]
                        if self.config.value_transformer:
                            values = self.config.value_transformer(parent_id, child_doc)
                        else:
                            values = self._default_transform(parent_id, child_doc)
                        batch_values.append(values)
                    else:
                        summary.record_error(config.table_name, 'Child document not found')
            
            if batch_values:
                placeholders = ', '.join(['%s'] * len(self.config.sql_columns))
                sql = f"""INSERT INTO {config.table_name} ({', '.join(self.config.sql_columns)}) 
                         VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"""
                
                try:
                    cursor.executemany(sql, batch_values)
                    summary.record_success(config.table_name, len(batch_values))
                    total_children += len(batch_values)
                    conn.commit()
                    
                except psycopg2.IntegrityError:
                    successful_count = ImportUtils.handle_batch_errors(
                        conn, cursor, sql, batch_values, config.table_name, config.summary_instance
                    )
                    total_children += successful_count
            
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