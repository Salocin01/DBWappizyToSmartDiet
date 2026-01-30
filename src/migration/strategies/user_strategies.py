from src.migration.import_strategies import DeleteAndInsertStrategy, SmartDiffStrategy, DirectTranslationStrategy, ImportConfig
from src.migration.repositories.mongo_repo import MongoRepository
from datetime import datetime


def create_user_events_strategy():
    """Create strategy for user_events array extraction with delete-and-insert pattern"""

    class UserEventsStrategy(DeleteAndInsertStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count users that have registered_events array"""
            mongo_filter = {'registered_events': {'$exists': True, '$ne': []}}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get user documents with registered_events array"""
            mongo_filter = {'registered_events': {'$exists': True, '$ne': []}}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'registered_events': 1, 'creation_date': 1, 'update_date': 1}
            ).sort('creation_date', 1).skip(offset).limit(config.batch_size))

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
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get user documents with target arrays"""
            mongo_filter = {'$or': [
                {'targets': {'$exists': True, '$ne': []}},
                {'specificity_targets': {'$exists': True, '$ne': []}},
                {'health_targets': {'$exists': True, '$ne': []}}
            ]}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'targets': 1, 'specificity_targets': 1, 'health_targets': 1, 'creation_date': 1, 'update_date': 1}
            ).sort('creation_date', 1).skip(offset).limit(config.batch_size))

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


def create_user_events_smart_strategy():
    """
    Create SMART strategy for user_events with intelligent diff-based optimization.

    Performance improvement over DeleteAndInsertStrategy:
    - Typical case (user adds 1 event to 5 existing): 2 ops instead of 12 ops (6x faster)
    - Worst case (user replaces all events): Same as delete-and-insert
    - Uses diff-based for <= 30% changes, full replace for > 30% changes
    """

    class UserEventsSmartStrategy(SmartDiffStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count users that have registered_events array"""
            mongo_filter = {'registered_events': {'$exists': True, '$ne': []}}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get user documents with registered_events array"""
            mongo_filter = {'registered_events': {'$exists': True, '$ne': []}}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'registered_events': 1, 'creation_date': 1, 'update_date': 1}
            ).sort('creation_date', 1).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Not used in SmartDiffStrategy (uses extract_current_items instead)"""
            return None, None

        def get_parent_id_from_document(self, document) -> str:
            """Extract user_id from user document"""
            return str(document['_id'])

        def get_child_column_name(self) -> str:
            """Column name for event ID"""
            return 'event_id'

        def get_parent_column_name(self) -> str:
            """Column name for user ID"""
            return 'user_id'

        def extract_current_items(self, document) -> set:
            """Extract current event IDs from MongoDB document as a set"""
            items = set()
            for event_item in document.get('registered_events', []):
                # Handle both ObjectId and embedded document formats
                if hasattr(event_item, 'get'):
                    event_id = str(event_item.get('event', event_item.get('_id', event_item)))
                else:
                    event_id = str(event_item)
                items.add((event_id,))  # Return as tuple for consistency
            return items

        def _item_to_sql_values(self, parent_id: str, item: tuple):
            """Convert item tuple to SQL values"""
            event_id = item[0]
            now = datetime.now()
            return (
                [parent_id, event_id, now, now],
                ['user_id', 'event_id', 'created_at', 'updated_at']
            )

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for user events"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} users, {total_records} user-event relationships"

    return UserEventsSmartStrategy()


def create_users_targets_smart_strategy():
    """
    Create SMART strategy for users_targets with intelligent diff-based optimization.

    Handles three arrays with type discrimination: targets (basic), specificity_targets, health_targets

    Performance improvement over DeleteAndInsertStrategy:
    - Typical case (user adds 1 target to 50 existing): 2 ops instead of 102 ops (51x faster)
    - Worst case (user replaces all targets): Same as delete-and-insert
    """

    class UsersTargetsSmartStrategy(SmartDiffStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count users that have any target arrays"""
            mongo_filter = {'$or': [
                {'targets': {'$exists': True, '$ne': []}},
                {'specificity_targets': {'$exists': True, '$ne': []}},
                {'health_targets': {'$exists': True, '$ne': []}}
            ]}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get user documents with target arrays"""
            mongo_filter = {'$or': [
                {'targets': {'$exists': True, '$ne': []}},
                {'specificity_targets': {'$exists': True, '$ne': []}},
                {'health_targets': {'$exists': True, '$ne': []}}
            ]}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'targets': 1, 'specificity_targets': 1, 'health_targets': 1, 'creation_date': 1, 'update_date': 1}
            ).sort('creation_date', 1).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Not used in SmartDiffStrategy (uses extract_current_items instead)"""
            return None, None

        def get_parent_id_from_document(self, document) -> str:
            """Extract user_id from user document"""
            return str(document['_id'])

        def get_child_column_name(self) -> str:
            """Column name for target ID"""
            return 'target_id'

        def get_parent_column_name(self) -> str:
            """Column name for user ID"""
            return 'user_id'

        def get_additional_columns(self) -> list:
            """Return additional columns for composite key (includes 'type')"""
            return ['type']

        def extract_current_items(self, document) -> set:
            """
            Extract current target IDs with type discrimination from MongoDB document.

            Returns set of tuples: {('target_id1', 'basic'), ('target_id2', 'health'), ...}
            """
            items = set()

            # Extract basic targets
            for target_id in document.get('targets', []):
                items.add((str(target_id), 'basic'))

            # Extract specificity targets
            for target_id in document.get('specificity_targets', []):
                items.add((str(target_id), 'specificity'))

            # Extract health targets
            for target_id in document.get('health_targets', []):
                items.add((str(target_id), 'health'))

            return items

        def _item_to_sql_values(self, parent_id: str, item: tuple):
            """Convert item tuple to SQL values (includes type)"""
            target_id, target_type = item
            now = datetime.now()
            return (
                [parent_id, target_id, target_type, now, now],
                ['user_id', 'target_id', 'type', 'created_at', 'updated_at']
            )

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for user targets"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} users, {total_records} user-target relationships"

    return UsersTargetsSmartStrategy()
