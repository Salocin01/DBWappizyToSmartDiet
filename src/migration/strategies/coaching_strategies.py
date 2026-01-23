from src.migration.import_strategies import DeleteAndInsertStrategy, ImportConfig
from src.migration.repositories.mongo_repo import MongoRepository


def create_days_contents_links_strategy():
    """Create strategy for days_contents_links array extraction with delete-and-insert pattern"""

    class DaysContentsLinksStrategy(DeleteAndInsertStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count days that have contents array"""
            mongo_filter = {'contents': {'$exists': True, '$ne': []}}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get day documents with contents array"""
            mongo_filter = {'contents': {'$exists': True, '$ne': []}}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'contents': 1, 'creation_date': 1, 'update_date': 1}
            ).sort('creation_date', 1).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Extract all content links from a day document"""
            day_id = str(document['_id'])
            creation_date = document.get('creation_date')
            update_date = document.get('update_date')

            batch_values = []

            # Extract contents
            for content_item in document.get('contents', []):
                # Handle both ObjectId and embedded document formats
                if hasattr(content_item, 'get'):
                    # It's a document with potential fields
                    content_id = str(content_item.get('content', content_item.get('_id', content_item)))
                else:
                    # It's just an ObjectId
                    content_id = str(content_item)

                batch_values.append([
                    day_id,
                    content_id,
                    creation_date,
                    update_date or creation_date
                ])

            return batch_values, ['day_id', 'content_id', 'created_at', 'updated_at']

        def get_parent_id_from_document(self, document) -> str:
            """Extract day_id from day document"""
            return str(document['_id'])

        def get_delete_table_name(self, config: ImportConfig) -> str:
            """Table to delete from is days_contents_links"""
            return 'days_contents_links'

        def get_delete_column_name(self) -> str:
            """Delete based on day_id column"""
            return 'day_id'

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for day-content links"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} days, {total_records} day-content links"

    return DaysContentsLinksStrategy()


def create_days_logbooks_links_strategy():
    """Create strategy for days_logbooks_links array extraction with delete-and-insert pattern"""

    class DaysLogbooksLinksStrategy(DeleteAndInsertStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count days that have main_logbooks array"""
            mongo_filter = {'main_logbooks': {'$exists': True, '$ne': []}}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get day documents with main_logbooks array"""
            mongo_filter = {'main_logbooks': {'$exists': True, '$ne': []}}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'main_logbooks': 1, 'creation_date': 1, 'update_date': 1}
            ).sort('creation_date', 1).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Extract all logbook links from a day document"""
            day_id = str(document['_id'])
            creation_date = document.get('creation_date')
            update_date = document.get('update_date')

            batch_values = []

            # Extract main logbooks
            for logbook_item in document.get('main_logbooks', []):
                # Handle both ObjectId and embedded document formats
                if hasattr(logbook_item, 'get'):
                    # It's a document with potential fields
                    logbook_id = str(logbook_item.get('logbook', logbook_item.get('_id', logbook_item)))
                else:
                    # It's just an ObjectId
                    logbook_id = str(logbook_item)

                batch_values.append([
                    day_id,
                    logbook_id,
                    creation_date,
                    update_date or creation_date
                ])

            return batch_values, ['day_id', 'logbook_id', 'created_at', 'updated_at']

        def get_parent_id_from_document(self, document) -> str:
            """Extract day_id from day document"""
            return str(document['_id'])

        def get_delete_table_name(self, config: ImportConfig) -> str:
            """Table to delete from is days_logbooks_links"""
            return 'days_logbooks_links'

        def get_delete_column_name(self) -> str:
            """Delete based on day_id column"""
            return 'day_id'

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for day-logbook links"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} days, {total_records} day-logbook links"

    return DaysLogbooksLinksStrategy()
