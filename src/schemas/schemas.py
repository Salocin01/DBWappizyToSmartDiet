from .table_schemas import ColumnDefinition, BaseEntitySchema, TableSchema




def _create_user_events_strategy():
    """Create strategy for user_events array extraction with delete-and-insert pattern"""
    from src.migration.import_strategies import ImportStrategy, ImportConfig, ImportUtils
    from src.connections.mongo_connection import get_mongo_collection
    from datetime import datetime

    class UserEventsStrategy(ImportStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count users that have registered_events array"""
            mongo_filter = {'registered_events': {'$exists': True, '$ne': []}}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get user documents with registered_events array"""
            mongo_filter = {'registered_events': {'$exists': True, '$ne': []}}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'registered_events': 1, 'creation_date': 1, 'update_date': 1}
            ).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Extract all registered events from a user document"""
            user_id = str(document['_id'])
            creation_date = document.get('creation_date')
            update_date = document.get('update_date')

            batch_values = []

            # Extract registered events
            for event_item in document.get('registered_events', []):
                # Handle both ObjectId and embedded document formats
                if hasattr(event_item, 'get'):
                    # It's a document with potential fields
                    event_id = str(event_item.get('event', event_item.get('_id', event_item)))
                    event_date = event_item.get('date', creation_date)
                else:
                    # It's just an ObjectId
                    event_id = str(event_item)
                    event_date = creation_date

                batch_values.append([
                    user_id,
                    event_id,
                    event_date or creation_date,
                    update_date or event_date or creation_date
                ])

            return batch_values, ['user_id', 'event_id', 'created_at', 'updated_at']

        def get_use_on_conflict(self) -> bool:
            return True

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} users, {total_records} user-event relationships"

        def export_data(self, conn, collection, config: ImportConfig):
            """Incremental sync: only process changed users, delete their old relationships and insert fresh ones"""
            from src.migration.import_strategies import DIRECT_IMPORT, ImportUtils
            import os

            print("Starting incremental user_events sync...")

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
                batch_user_ids = []

                for doc in documents:
                    values, doc_columns = self.extract_data_for_sql(doc, config)
                    if values is not None:
                        if columns is None:
                            columns = doc_columns

                        user_id = str(doc['_id'])
                        batch_user_ids.append(user_id)

                        # Handle both single records and multiple records per document
                        if isinstance(values, list) and len(values) > 0 and isinstance(values[0], list):
                            batch_values.extend(values)
                        else:
                            batch_values.append(values)

                # Delete existing relationships for these specific users
                if batch_user_ids and DIRECT_IMPORT:
                    cursor = conn.cursor()
                    try:
                        placeholders = ', '.join(['%s'] * len(batch_user_ids))
                        delete_sql = f"DELETE FROM user_events WHERE user_id IN ({placeholders})"
                        cursor.execute(delete_sql, batch_user_ids)
                        deleted_count = cursor.rowcount
                        conn.commit()
                        print(f"Deleted {deleted_count} existing relationships for {len(batch_user_ids)} updated users")
                    except Exception as e:
                        print(f"Error deleting existing relationships: {e}")
                        conn.rollback()
                    finally:
                        cursor.close()

                # Insert fresh relationships
                if batch_values:
                    actual_insertions = ImportUtils.execute_batch(
                        conn, batch_values, columns, config.table_name,
                        config.summary_instance, use_on_conflict=False,
                        on_conflict_clause=""
                    )
                    total_records += actual_insertions

                    if DIRECT_IMPORT:
                        print(f"Inserted {actual_insertions} fresh relationships for {len(batch_user_ids)} users")
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
                    executed_count = ImportUtils.execute_sql_file(conn, sql_file_path, config.summary_instance)
                    os.remove(sql_file_path)
                    print(f"Executed and deleted SQL file: {sql_file_path} ({executed_count} statements)")
                    return executed_count

            return total_records

    return UserEventsStrategy()


def _create_users_targets_strategy():
    """Create strategy for users_targets array extraction from multiple target fields"""
    from src.migration.import_strategies import ImportStrategy, ImportConfig, ImportUtils
    from src.connections.mongo_connection import get_mongo_collection
    from datetime import datetime
    
    class UsersTargetsStrategy(ImportStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count users that have any target arrays"""
            mongo_filter = {'$or': [
                {'targets': {'$exists': True, '$ne': []}},
                {'specificity_targets': {'$exists': True, '$ne': []}},
                {'health_targets': {'$exists': True, '$ne': []}}
            ]}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)
        
        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get user documents with target arrays"""
            mongo_filter = {'$or': [
                {'targets': {'$exists': True, '$ne': []}},
                {'specificity_targets': {'$exists': True, '$ne': []}},
                {'health_targets': {'$exists': True, '$ne': []}}
            ]}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))
            
            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'targets': 1, 'specificity_targets': 1, 'health_targets': 1, 'creation_date': 1, 'update_date': 1}
            ).skip(offset).limit(config.batch_size))
        
        def extract_data_for_sql(self, document, config: ImportConfig):
            """Extract all target relationships from a user document"""
            user_id = str(document['_id'])
            creation_date = document.get('creation_date')
            update_date = document.get('update_date')
            
            batch_values = []
            
            # Extract basic targets
            for target_id in document.get('targets', []):
                batch_values.append([
                    user_id,
                    str(target_id),
                    'basic',
                    creation_date,
                    update_date
                ])
            
            # Extract specificity targets
            for target_id in document.get('specificity_targets', []):
                batch_values.append([
                    user_id,
                    str(target_id),
                    'specificity',
                    creation_date,
                    update_date
                ])
            
            # Extract health targets
            for target_id in document.get('health_targets', []):
                batch_values.append([
                    user_id,
                    str(target_id),
                    'health',
                    creation_date,
                    update_date
                ])
            
            return batch_values, ['user_id', 'target_id', 'type', 'created_at', 'updated_at']
        
        def get_use_on_conflict(self) -> bool:
            return True
        
        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} users, {total_records} user-target relationships"
        
        def export_data(self, conn, collection, config: ImportConfig):
            """Incremental sync: only process changed users, delete their old relationships and insert fresh ones"""
            from src.migration.import_strategies import DIRECT_IMPORT, ImportUtils
            import os
            
            print("Starting incremental users_targets sync...")
            
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
                batch_user_ids = []
                
                for doc in documents:
                    values, doc_columns = self.extract_data_for_sql(doc, config)
                    if values is not None:
                        if columns is None:
                            columns = doc_columns
                        
                        user_id = str(doc['_id'])
                        batch_user_ids.append(user_id)
                        
                        # Handle both single records and multiple records per document
                        if isinstance(values, list) and len(values) > 0 and isinstance(values[0], list):
                            batch_values.extend(values)
                        else:
                            batch_values.append(values)
                
                # Delete existing relationships for these specific users
                if batch_user_ids and DIRECT_IMPORT:
                    cursor = conn.cursor()
                    try:
                        placeholders = ', '.join(['%s'] * len(batch_user_ids))
                        delete_sql = f"DELETE FROM users_targets WHERE user_id IN ({placeholders})"
                        cursor.execute(delete_sql, batch_user_ids)
                        deleted_count = cursor.rowcount
                        conn.commit()
                        print(f"Deleted {deleted_count} existing relationships for {len(batch_user_ids)} updated users")
                    except Exception as e:
                        print(f"Error deleting existing relationships: {e}")
                        conn.rollback()
                    finally:
                        cursor.close()
                
                # Insert fresh relationships
                if batch_values:
                    actual_insertions = ImportUtils.execute_batch(
                        conn, batch_values, columns, config.table_name, 
                        config.summary_instance, use_on_conflict=False,
                        on_conflict_clause=""
                    )
                    total_records += actual_insertions
                    
                    if DIRECT_IMPORT:
                        print(f"Inserted {actual_insertions} fresh relationships for {len(batch_user_ids)} users")
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
                    executed_count = ImportUtils.execute_sql_file(conn, sql_file_path, config.summary_instance)
                    os.remove(sql_file_path)
                    print(f"Executed and deleted SQL file: {sql_file_path} ({executed_count} statements)")
                    return executed_count
            
            return total_records
    
    return UsersTargetsStrategy()


def create_schemas():
    schemas = {
        'ingredients': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
            ],
            export_order=1
        ),
        
        'appointment_types': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('title', 'VARCHAR(255)', nullable=False),
            ],
            mongo_collection='appointmenttypes',
            export_order=1
        ),
        
        'companies': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
            ],
            export_order=1
        ),
        
        'offers': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('duration', 'SMALLINT', nullable=False),
                ColumnDefinition('coaching_credit', 'SMALLINT', nullable=False),
            ],
            export_order=1
        ),
        
        'categories': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('type', 'VARCHAR(255)', nullable=False),
            ],
            export_order=1
        ),
        
        'targets': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
            ],
            export_order=1
        ),
        
        'events': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('type', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('start_date', 'TIMESTAMP', nullable=False),
                ColumnDefinition('end_date', 'TIMESTAMP', nullable=False),
                ColumnDefinition('company_id', 'VARCHAR', foreign_key='companies(id)')
            ],
            additional_mappings={
                'company': 'company_id',
                '__t': 'type',
            },
            export_order=2
        ),
        
        'users': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('firstname', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('lastname', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('email', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('birthdate', 'DATE'),
                ColumnDefinition('company_id', 'VARCHAR', foreign_key='companies(id)'),
                ColumnDefinition('role', 'VARCHAR(100)', nullable=False),
            ],
            additional_mappings={
                'company': 'company_id'
            },
            export_order=2
        ),
        
        
        'user_events': TableSchema.create(
            columns=[
                ColumnDefinition('user_id', 'VARCHAR', nullable=False, foreign_key='users(id)'),
                ColumnDefinition('event_id', 'VARCHAR', nullable=False, foreign_key='events(id)'),
                ColumnDefinition('created_at', 'TIMESTAMP', nullable=False),
                ColumnDefinition('updated_at', 'TIMESTAMP', nullable=False)
            ],
            mongo_collection='users',
            explicit_mappings={
                'creation_date': 'created_at',
                'update_date': 'updated_at'
            },
            export_order=3,
            import_strategy=_create_user_events_strategy(),
            unique_constraints=[['user_id', 'event_id']]
        ),
        
        'users_targets': TableSchema.create(
            columns=[
                ColumnDefinition('user_id', 'VARCHAR', nullable=False, foreign_key='users(id)'),
                ColumnDefinition('target_id', 'VARCHAR', nullable=False, foreign_key='targets(id)'),
                ColumnDefinition('type', 'VARCHAR(50)', nullable=False),
                ColumnDefinition('created_at', 'TIMESTAMP', nullable=False),
                ColumnDefinition('updated_at', 'TIMESTAMP', nullable=False)
            ],
            mongo_collection='users',
            explicit_mappings={
                'creation_date': 'created_at',
                'update_date': 'updated_at'
            },
            export_order=3,
            import_strategy=_create_users_targets_strategy(),
            unique_constraints=[['user_id', 'target_id', 'type']]
        ),
        
        
        'messages': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('sender_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('receiver_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('content', 'TEXT', nullable=False),
            ],
            additional_mappings={
                'sender': 'sender_id',
                'receiver': 'receiver_id'
            },
            export_order=3
        ),
        
        'coachings': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('user_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('diet_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('offer_id', 'VARCHAR', foreign_key='offers(id)'),
                ColumnDefinition('status', 'VARCHAR(100)', nullable=False),
            ],
            additional_mappings={
                'user': 'user_id',
                'diet': 'diet_id',
                'offer': 'offer_id'
            },
            export_order=3
        ),
        
        
        'appointments': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('user_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('coaching_id', 'VARCHAR', foreign_key='coachings(id)'),
                ColumnDefinition('type_id', 'VARCHAR', foreign_key='appointment_types(id)'),
                ColumnDefinition('start_date', 'DATE', nullable=False),
                ColumnDefinition('end_date', 'DATE', nullable=False),
                ColumnDefinition('validated', 'BOOLEAN'),
                ColumnDefinition('order_nb', 'SMALLINT', nullable=False),
            ],
            additional_mappings={
                'user': 'user_id',
                'coaching': 'coaching_id',
                'appointment_type': 'type_id',
                'order': 'order_nb'
            },
            export_order=4
        ),
        
        
        
        'recipes': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
            ],
            export_order=1
        ),
        
        'menus': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
            ],
            export_order=2
        ),
        
        'menu_recipes': TableSchema.create(
            columns=[
                ColumnDefinition('menu_id', 'VARCHAR', nullable=False, foreign_key='menus(id)'),
                ColumnDefinition('recipe_id', 'VARCHAR', nullable=False, foreign_key='recipes(id)'),
                ColumnDefinition('created_at', 'TIMESTAMP', nullable=False),
                ColumnDefinition('updated_at', 'TIMESTAMP', nullable=False)
            ],
            mongo_collection='menu_recipes',
            explicit_mappings={
                'creation_date': 'created_at',
                'update_date': 'updated_at',
                'menu': 'menu_id',
                'recipe': 'recipe_id'
            },
            export_order=3,
            unique_constraints=[['menu_id', 'recipe_id']]
        )
    }
    
    # Set table names from schema keys if not provided
    for key, schema in schemas.items():
        if schema.name is None:
            schema.name = key
            # Also update mongo_collection if it was None
            if schema.mongo_collection is None:
                schema.mongo_collection = key
    
    return schemas

TABLE_SCHEMAS = create_schemas()