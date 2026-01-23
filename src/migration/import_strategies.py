"""
Migration Import Strategies

This module implements the 4-step migration pattern used to transfer data from MongoDB to PostgreSQL:

STEP 1: Get Last Migration Date
    - Query PostgreSQL for the latest created_at/updated_at timestamp
    - Location: transfert_data.py calls get_last_insert_date()
    - Returns: datetime or None (for full import)
    - Purpose: Enable incremental sync to avoid re-processing unchanged data

STEP 2: Query New/Updated Documents
    - Query MongoDB for documents created or updated after the last migration date
    - Implementation: strategy.count_total_documents() and strategy.get_documents()
    - Uses MongoRepository.build_date_filter() to construct MongoDB query with $gte operator
    - Filter: {$or: [{creation_date: {$gte: date}}, {update_date: {$gte: date}}]}
    - Purpose: Fetch only changed data since last migration

STEP 3: Transform Data
    - Convert MongoDB documents to PostgreSQL-compatible row data
    - Implementation: strategy.extract_data_for_sql()
    - Handles ObjectId conversion, field mapping, and data type transformation
    - Returns: (values, columns) tuple for SQL insertion
    - Purpose: Adapt document structure to relational schema

STEP 4: Execute Import
    - Insert/update records in PostgreSQL with error handling
    - Implementation: strategy.export_data() calls PostgresRepository.execute_batch()
    - Handles ON CONFLICT resolution, batch processing, and transaction management
    - Uses savepoints for atomic batch operations with fallback to individual inserts
    - Purpose: Persist data with integrity and error recovery

Strategy Types:

1. DirectTranslationStrategy
   - Simple 1:1 collection-to-table mapping
   - Uses ON CONFLICT DO UPDATE for upsert behavior
   - Automatically maps fields based on schema.field_mappings
   - Examples: users, companies, ingredients, events
   - Pattern: One document → One table row

2. ArrayExtractionStrategy
   - Extracts array fields into separate relationship tables
   - Uses ON CONFLICT DO UPDATE for upsert behavior
   - Handles both embedded documents and ObjectId references
   - Examples: menu_recipes (if stored as document arrays)
   - Pattern: One document with array → Multiple table rows

3. DeleteAndInsertStrategy (Base class)
   - Handles relationship tables where arrays must be completely refreshed
   - Uses delete-and-insert pattern instead of upsert
   - Ensures both additions AND removals are correctly synced
   - Subclasses: UserEventsStrategy, UsersTargetsStrategy
   - Pattern:
     1. Identify changed parent documents
     2. DELETE all relationships for those parents
     3. INSERT fresh relationships from current array state
   - Why delete-and-insert?
     * When a user unregisters from an event, it's removed from MongoDB array
     * Without deletion, the old relationship would remain orphaned in PostgreSQL
     * This pattern ensures PostgreSQL perfectly mirrors MongoDB's current state

Configuration:

- IMPORT_BY_BATCH (bool): Use batch processing (True) or single statements (False)
  * True: Faster but requires rollback on any error
  * False: Slower but isolates errors to individual records

- DIRECT_IMPORT (bool): Execute SQL directly (True) or generate SQL files (False)
  * True: Immediate execution with real-time error handling
  * False: Generate .sql files for review/manual execution

Error Handling:

- Batch failures trigger automatic fallback to individual insert retry
- Uses PostgreSQL savepoints for atomic rollback without losing progress
- Tracks errors by type: foreign key constraint, NULL constraint, other
- Continues processing after errors to maximize data import
- Comprehensive error summary provided after each table migration

Performance:

- Default batch size: 5000 documents per query
- Parallel document processing within batches
- Connection pooling for PostgreSQL
- Efficient MongoDB cursor-based pagination
- Progress tracking with real-time console output
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Any
from bson import ObjectId
from src.migration.repositories.mongo_repo import MongoRepository
from src.migration.repositories.postgres_repo import PostgresRepository

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
    
    def get_on_conflict_clause(self, table_name: str, columns: list = None) -> str:
        """Get the ON CONFLICT clause for the table. Override in subclasses if needed."""
        if not self.get_use_on_conflict():
            return ""

        # Try to get the table schema to determine the appropriate conflict clause
        try:
            from src.schemas.schemas import TABLE_SCHEMAS
            schema = TABLE_SCHEMAS.get(table_name)
            if schema:
                return schema.get_on_conflict_clause(columns)
        except Exception:
            pass

        # Fallback to default behavior with UPDATE
        if columns:
            # Build UPDATE SET clause for all columns except the conflict target
            update_columns = [col for col in columns if col != 'id']
            if update_columns:
                set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                return f" ON CONFLICT (id) DO UPDATE SET {set_clause}"

        return " ON CONFLICT (id) DO NOTHING"
    
    def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
        """Override in subclasses for custom progress messages"""
        return f"Processed {processed}/{total} documents for {table_name}"
    
    def export_data(self, conn, collection, config: ImportConfig):
        """Generic export implementation that works for both strategies"""
        import os

        postgres_repo = PostgresRepository(
            conn,
            summary_instance=config.summary_instance,
            import_by_batch=IMPORT_BY_BATCH,
            direct_import=DIRECT_IMPORT,
        )

        # Ensure SQL exports directory exists
        if not DIRECT_IMPORT:
            os.makedirs("sql_exports", exist_ok=True)

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
                actual_insertions = postgres_repo.execute_batch(
                    batch_values,
                    columns,
                    config.table_name,
                    use_on_conflict=self.get_use_on_conflict(),
                    on_conflict_clause=self.get_on_conflict_clause(config.table_name, columns),
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

        if not DIRECT_IMPORT:
            # Execute the accumulated SQL file as one entity row
            sql_file_path = f"sql_exports/{config.table_name}_import.sql"
            if os.path.exists(sql_file_path):
                executed_count = postgres_repo.execute_sql_file(sql_file_path)
                os.remove(sql_file_path)
                print(f"Executed and deleted SQL file: {sql_file_path} ({executed_count} statements)")
                return executed_count


class DirectTranslationStrategy(ImportStrategy):
    """Handles simple 1:1 collection-to-table imports using schema field mappings"""
    
    def count_total_documents(self, collection, config: ImportConfig) -> int:
        """Count total documents that will be processed"""
        return MongoRepository.count_documents(collection, config.after_date)
    
    def get_documents(self, collection, config: ImportConfig, offset: int = 0):
        """Get documents for processing with pagination"""
        return MongoRepository.find_documents(
            collection,
            after_date=config.after_date,
            offset=offset,
            limit=config.batch_size,
        )
    
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
    
    def get_use_on_conflict(self) -> bool:
        """Use ON CONFLICT for tables with primary keys or unique constraints"""
        return True
    
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
        parent_filter.update(MongoRepository.build_date_filter(config.after_date))
        
        return parent_collection.count_documents(parent_filter)
    
    def get_documents(self, collection, config: ImportConfig, offset: int = 0):
        """Get parent documents for processing with pagination"""
        from src.connections.mongo_connection import get_mongo_collection
        
        parent_collection = get_mongo_collection(self.config.parent_collection)
        parent_filter = {self.config.array_field: {'$exists': True, '$ne': []}}
        parent_filter.update(MongoRepository.build_date_filter(config.after_date))
        
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


class DeleteAndInsertStrategy(ImportStrategy):
    """
    Base class for strategies that use delete-and-insert pattern for relationship tables.

    This pattern is used when:
    - Documents contain arrays that represent relationships
    - Updates to the array should completely replace existing relationships
    - We need to handle both additions AND removals from the array

    The 4-step process:
    1. Get last migration date (handled by transfert_data.py)
    2. Query changed documents (implemented by subclasses)
    3. Extract relationship data (implemented by subclasses)
    4. Delete old + Insert fresh relationships (handled here)

    Subclasses must implement:
    - count_total_documents(): Count documents with changes
    - get_documents(): Fetch changed documents with pagination
    - extract_data_for_sql(): Transform document to SQL rows
    - get_parent_id_from_document(): Extract parent entity ID
    - get_delete_table_name(): Table name for deletion
    - get_delete_column_name(): Column name for WHERE clause
    """

    @abstractmethod
    def get_parent_id_from_document(self, document) -> str:
        """Extract the parent entity ID from a document (e.g., user_id from user document)"""
        pass

    @abstractmethod
    def get_delete_table_name(self, config: ImportConfig) -> str:
        """Return the table name to delete from (usually config.table_name)"""
        pass

    @abstractmethod
    def get_delete_column_name(self) -> str:
        """Return the column name to use in DELETE WHERE clause (e.g., 'user_id')"""
        pass

    def export_data(self, conn, collection, config: ImportConfig):
        """
        Template method implementing the 4-step delete-and-insert pattern:
        1. Query changed documents (via get_documents)
        2. Extract relationship data (via extract_data_for_sql)
        3. Delete existing relationships for changed parents
        4. Insert fresh relationships
        """
        import os

        print(f"Starting incremental {config.table_name} sync...")

        postgres_repo = PostgresRepository(
            conn,
            summary_instance=config.summary_instance,
            import_by_batch=IMPORT_BY_BATCH,
            direct_import=DIRECT_IMPORT,
        )

        # Ensure SQL exports directory exists
        if not DIRECT_IMPORT:
            os.makedirs("sql_exports", exist_ok=True)

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
            batch_parent_ids = []

            # Step 2 & 3: Extract data from documents
            for doc in documents:
                values, doc_columns = self.extract_data_for_sql(doc, config)
                if values is not None:
                    if columns is None:
                        columns = doc_columns

                    parent_id = self.get_parent_id_from_document(doc)
                    batch_parent_ids.append(parent_id)

                    # Handle both single records and multiple records per document
                    if isinstance(values, list) and len(values) > 0 and isinstance(values[0], list):
                        batch_values.extend(values)
                    else:
                        batch_values.append(values)

            # Step 3: Delete existing relationships for changed parents
            if batch_parent_ids and DIRECT_IMPORT:
                self._delete_existing_relationships(postgres_repo, batch_parent_ids, config)

            # Step 4: Insert fresh relationships
            if batch_values:
                actual_insertions = postgres_repo.execute_batch(
                    batch_values,
                    columns,
                    config.table_name,
                    use_on_conflict=False,
                    on_conflict_clause="",
                )
                total_records += actual_insertions

                if DIRECT_IMPORT:
                    print(f"Inserted {actual_insertions} fresh relationships for {len(batch_parent_ids)} parents")
                    print(self.get_progress_message(
                        processed_docs + len(documents), total_docs, config.table_name,
                        total_records=total_records
                    ))
                else:
                    print(f"Generated SQL for {total_records} records from {processed_docs + len(documents)}/{total_docs} documents for {config.table_name}")

            processed_docs += len(documents)
            offset += config.batch_size

            if len(documents) < config.batch_size:
                break

        action = "processing" if DIRECT_IMPORT else "SQL generation for"
        print(f"Completed incremental {action} {total_records} records from {processed_docs} documents for {config.table_name}")

        if not DIRECT_IMPORT:
            # Execute the accumulated SQL file
            sql_file_path = f"sql_exports/{config.table_name}_import.sql"
            if os.path.exists(sql_file_path):
                executed_count = postgres_repo.execute_sql_file(sql_file_path)
                os.remove(sql_file_path)
                print(f"Executed and deleted SQL file: {sql_file_path} ({executed_count} statements)")
                return executed_count

        return total_records

    def _delete_existing_relationships(self, postgres_repo, parent_ids: List[str], config: ImportConfig):
        """Delete existing relationships for the specified parent IDs"""
        table_name = self.get_delete_table_name(config)
        column_name = self.get_delete_column_name()
        try:
            deleted_count = postgres_repo.delete_by_parent_ids(
                table_name,
                column_name,
                parent_ids,
            )
            print(f"Deleted {deleted_count} existing relationships for {len(parent_ids)} updated parents")
        except Exception as e:
            print(f"Error deleting existing relationships: {e}")

    def get_use_on_conflict(self) -> bool:
        """Delete-and-insert doesn't use ON CONFLICT"""
        return False
