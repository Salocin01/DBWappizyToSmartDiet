from src.migration.import_strategies import DeleteAndInsertStrategy, DirectTranslationStrategy, ImportConfig, ImportUtils


def create_user_events_strategy():
    """Create strategy for user_events array extraction with delete-and-insert pattern"""

    class UserEventsStrategy(DeleteAndInsertStrategy):
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

        def get_parent_id_from_document(self, document) -> str:
            """Extract user_id from user document"""
            return str(document['_id'])

        def get_delete_table_name(self, config: ImportConfig) -> str:
            """Table to delete from is user_events"""
            return 'user_events'

        def get_delete_column_name(self) -> str:
            """Delete based on user_id column"""
            return 'user_id'

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for user events"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} users, {total_records} user-event relationships"

    return UserEventsStrategy()


def create_users_logbook_strategy():
    """Create strategy for users_logbook with user field filter"""

    class UsersLogbookStrategy(DirectTranslationStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count documents that have a user field (not None)"""
            mongo_filter = {'user': {'$exists': True, '$ne': None}}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get documents with user field"""
            mongo_filter = {'user': {'$exists': True, '$ne': None}}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))
            return list(collection.find(mongo_filter).skip(offset).limit(config.batch_size))

    return UsersLogbookStrategy()


def create_users_targets_strategy():
    """Create strategy for users_targets array extraction from multiple target fields"""

    class UsersTargetsStrategy(DeleteAndInsertStrategy):
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

        def get_parent_id_from_document(self, document) -> str:
            """Extract user_id from user document"""
            return str(document['_id'])

        def get_delete_table_name(self, config: ImportConfig) -> str:
            """Table to delete from is users_targets"""
            return 'users_targets'

        def get_delete_column_name(self) -> str:
            """Delete based on user_id column"""
            return 'user_id'

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for user targets"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} users, {total_records} user-target relationships"

    return UsersTargetsStrategy()
