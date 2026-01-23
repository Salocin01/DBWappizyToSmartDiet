from src.migration.import_strategies import DeleteAndInsertStrategy, ImportConfig
from src.migration.repositories.mongo_repo import MongoRepository


def create_users_contents_reads_strategy():
    """Create strategy for users_contents_reads array extraction with delete-and-insert pattern"""

    class UsersContentsReadsStrategy(DeleteAndInsertStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count contents that have viewed_by array"""
            mongo_filter = {'viewed_by': {'$exists': True, '$ne': []}}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get content documents with viewed_by array"""
            mongo_filter = {'viewed_by': {'$exists': True, '$ne': []}}
            mongo_filter.update(MongoRepository.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'viewed_by': 1, 'creation_date': 1, 'update_date': 1}
            ).sort('creation_date', 1).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Extract all viewed_by relationships from a content document"""
            content_id = str(document['_id'])
            creation_date = document.get('creation_date')
            update_date = document.get('update_date')

            batch_values = []

            # Extract viewed_by users
            for user_item in document.get('viewed_by', []):
                # Handle both ObjectId and embedded document formats
                if hasattr(user_item, 'get'):
                    # It's a document with potential fields
                    user_id = str(user_item.get('user', user_item.get('_id', user_item)))
                else:
                    # It's just an ObjectId
                    user_id = str(user_item)

                batch_values.append([
                    content_id,
                    user_id,
                    creation_date,
                    update_date or creation_date
                ])

            return batch_values, ['content_id', 'user_id', 'created_at', 'updated_at']

        def get_parent_id_from_document(self, document) -> str:
            """Extract content_id from content document"""
            return str(document['_id'])

        def get_delete_table_name(self, config: ImportConfig) -> str:
            """Table to delete from is users_contents_reads"""
            return 'users_contents_reads'

        def get_delete_column_name(self) -> str:
            """Delete based on content_id column"""
            return 'content_id'

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for content reads"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} contents, {total_records} content-read relationships"

    return UsersContentsReadsStrategy()
