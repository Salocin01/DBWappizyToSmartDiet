from src.migration.import_strategies import DeleteAndInsertStrategy, ImportConfig, ImportUtils


def create_users_quizzs_links_questions_strategy():
    """Create strategy for users_quizzs_links_questions array extraction with delete-and-insert pattern"""

    class UsersQuizzsLinksQuestionsStrategy(DeleteAndInsertStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count user quizzes that have questions array"""
            mongo_filter = {'questions': {'$exists': True, '$ne': []}}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get user quiz documents with questions array"""
            mongo_filter = {'questions': {'$exists': True, '$ne': []}}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'questions': 1, 'creation_date': 1, 'update_date': 1}
            ).sort('creation_date', 1).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Extract all question relationships from a user quiz document"""
            user_quizz_id = str(document['_id'])
            creation_date = document.get('creation_date')
            update_date = document.get('update_date')

            batch_values = []

            # Extract questions
            for question_item in document.get('questions', []):
                # Handle both ObjectId and embedded document formats
                if hasattr(question_item, 'get'):
                    # It's a document with potential fields
                    user_quizz_question_id = str(question_item.get('question', question_item.get('_id', question_item)))
                else:
                    # It's just an ObjectId
                    user_quizz_question_id = str(question_item)

                batch_values.append([
                    user_quizz_id,
                    user_quizz_question_id,
                    creation_date,
                    update_date or creation_date
                ])

            return batch_values, ['user_quizz_id', 'user_quizz_question_id', 'created_at', 'updated_at']

        def get_parent_id_from_document(self, document) -> str:
            """Extract user_quizz_id from user quiz document"""
            return str(document['_id'])

        def get_delete_table_name(self, config: ImportConfig) -> str:
            """Table to delete from is users_quizzs_links_questions"""
            return 'users_quizzs_links_questions'

        def get_delete_column_name(self) -> str:
            """Delete based on user_quizz_id column"""
            return 'user_quizz_id'

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for user quiz-question links"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} user quizzes, {total_records} user quiz-question relationships"

    return UsersQuizzsLinksQuestionsStrategy()


def create_quizzs_links_questions_strategy():
    """Create strategy for quizzs_links_questions array extraction with delete-and-insert pattern"""

    class QuizzsLinksQuestionsStrategy(DeleteAndInsertStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count quizzes that have questions array"""
            mongo_filter = {'questions': {'$exists': True, '$ne': []}}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get quiz documents with questions array"""
            mongo_filter = {'questions': {'$exists': True, '$ne': []}}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'questions': 1, 'creation_date': 1, 'update_date': 1}
            ).sort('creation_date', 1).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Extract all question relationships from a quiz document"""
            quizz_id = str(document['_id'])
            creation_date = document.get('creation_date')
            update_date = document.get('update_date')

            batch_values = []

            # Extract questions
            for question_item in document.get('questions', []):
                # Handle both ObjectId and embedded document formats
                if hasattr(question_item, 'get'):
                    # It's a document with potential fields
                    question_id = str(question_item.get('question', question_item.get('_id', question_item)))
                else:
                    # It's just an ObjectId
                    question_id = str(question_item)

                batch_values.append([
                    quizz_id,
                    question_id,
                    creation_date,
                    update_date or creation_date
                ])

            return batch_values, ['quizz_id', 'question_id', 'created_at', 'updated_at']

        def get_parent_id_from_document(self, document) -> str:
            """Extract quizz_id from quiz document"""
            return str(document['_id'])

        def get_delete_table_name(self, config: ImportConfig) -> str:
            """Table to delete from is quizzs_links_questions"""
            return 'quizzs_links_questions'

        def get_delete_column_name(self) -> str:
            """Delete based on quizz_id column"""
            return 'quizz_id'

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for quiz-question links"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} quizzes, {total_records} quiz-question relationships"

    return QuizzsLinksQuestionsStrategy()
