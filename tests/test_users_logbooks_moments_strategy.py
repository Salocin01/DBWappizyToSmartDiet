"""
Tests for UsersLogbooksMomentsStrategy and UsersLogbooksMomentsDetailsStrategy

Tests the custom strategies for migrating userquizzs to users_logbooks_moments
and users_logbooks_moments_details with filtering for QUIZZ_TYPE_LOGBOOK and non-empty text.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from bson import ObjectId
from datetime import datetime

from src.schemas.schemas import _create_users_logbooks_moments_strategy, _create_users_logbooks_moments_details_strategy
from src.migration.import_strategies import ImportConfig


class TestUsersLogbooksMomentsStrategy:
    """Test suite for UsersLogbooksMomentsStrategy"""

    @pytest.fixture
    def strategy(self):
        """Create a UsersLogbooksMomentsStrategy instance"""
        return _create_users_logbooks_moments_strategy()

    @pytest.fixture
    def mock_collection(self):
        """Create a mock MongoDB collection"""
        collection = Mock()
        collection.count_documents = Mock(return_value=1)
        collection.find = Mock()
        return collection

    @pytest.fixture
    def import_config(self):
        """Create a test ImportConfig"""
        return ImportConfig(
            table_name='users_logbooks_moments',
            source_collection='userquizzs',
            batch_size=5000,
            after_date=datetime(2024, 1, 1),
            summary_instance=Mock()
        )

    @patch('src.schemas.schemas.get_mongo_collection')
    @patch('src.schemas.schemas.connect_postgres')
    def test_extract_with_valid_logbook_question(self, mock_pg_conn, mock_mongo_get, strategy, import_config):
        """Test extracting data with valid QUIZZ_TYPE_LOGBOOK question and non-empty text"""
        userquizz_id = ObjectId()
        question_id = ObjectId()
        quizz_question_id = ObjectId()
        user_id = ObjectId()

        document = {
            '_id': userquizz_id,
            'name': 'Morning Check-in',
            'questions': [question_id],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        # Mock MongoDB collections
        mock_userquizzquestions = Mock()
        mock_userquizzquestions.find_one.return_value = {
            '_id': question_id,
            'quizzQuestion': quizz_question_id
        }

        mock_quizzquestions = Mock()
        mock_quizzquestions.find_one.return_value = {
            '_id': quizz_question_id,
            'type': 'QUIZZ_TYPE_LOGBOOK',
            'title': 'How are you feeling?'
        }

        mock_items = Mock()
        mock_items.count_documents.return_value = 2  # Has non-empty items

        mock_coachinglogbooks = Mock()
        mock_coachinglogbooks.find_one.return_value = {
            'user': user_id,
            'day': datetime(2024, 1, 15).date()
        }

        def get_collection_side_effect(name):
            if name == 'userquizzquestions':
                return mock_userquizzquestions
            elif name == 'quizzquestions':
                return mock_quizzquestions
            elif name == 'items':
                return mock_items
            elif name == 'coachinglogbooks':
                return mock_coachinglogbooks
            return Mock()

        mock_mongo_get.side_effect = get_collection_side_effect

        # Mock PostgreSQL connection
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (123,)  # users_logbook.id
        mock_pg_conn.return_value.cursor.return_value = mock_cursor

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify successful extraction
        assert values is not None
        assert columns == ['id', 'user_logbook_id', 'type', 'created_at', 'updated_at']
        assert values[0] == str(userquizz_id)
        assert values[1] == 123  # user_logbook_id from PostgreSQL
        assert values[2] == 'Morning Check-in'
        assert values[3] == datetime(2024, 1, 15)
        assert values[4] == datetime(2024, 1, 20)

    @patch('src.schemas.schemas.get_mongo_collection')
    def test_extract_skips_non_logbook_questions(self, mock_mongo_get, strategy, import_config):
        """Test that userquizzs with only non-LOGBOOK questions are skipped"""
        userquizz_id = ObjectId()
        question_id = ObjectId()
        quizz_question_id = ObjectId()

        document = {
            '_id': userquizz_id,
            'name': 'Diet Quiz',
            'questions': [question_id],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        # Mock MongoDB collections
        mock_userquizzquestions = Mock()
        mock_userquizzquestions.find_one.return_value = {
            '_id': question_id,
            'quizzQuestion': quizz_question_id
        }

        mock_quizzquestions = Mock()
        mock_quizzquestions.find_one.return_value = {
            '_id': quizz_question_id,
            'type': 'QUIZZ_TYPE_DIET',  # Wrong type!
            'title': 'What is your diet?'
        }

        def get_collection_side_effect(name):
            if name == 'userquizzquestions':
                return mock_userquizzquestions
            elif name == 'quizzquestions':
                return mock_quizzquestions
            return Mock()

        mock_mongo_get.side_effect = get_collection_side_effect

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should be skipped (no valid questions)
        assert values is None
        assert columns is None

    @patch('src.schemas.schemas.get_mongo_collection')
    def test_extract_skips_empty_items(self, mock_mongo_get, strategy, import_config):
        """Test that userquizzs with only empty items are skipped"""
        userquizz_id = ObjectId()
        question_id = ObjectId()
        quizz_question_id = ObjectId()

        document = {
            '_id': userquizz_id,
            'name': 'Morning Check-in',
            'questions': [question_id],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        # Mock MongoDB collections
        mock_userquizzquestions = Mock()
        mock_userquizzquestions.find_one.return_value = {
            '_id': question_id,
            'quizzQuestion': quizz_question_id
        }

        mock_quizzquestions = Mock()
        mock_quizzquestions.find_one.return_value = {
            '_id': quizz_question_id,
            'type': 'QUIZZ_TYPE_LOGBOOK',
            'title': 'How are you feeling?'
        }

        mock_items = Mock()
        mock_items.count_documents.return_value = 0  # No non-empty items!

        def get_collection_side_effect(name):
            if name == 'userquizzquestions':
                return mock_userquizzquestions
            elif name == 'quizzquestions':
                return mock_quizzquestions
            elif name == 'items':
                return mock_items
            return Mock()

        mock_mongo_get.side_effect = get_collection_side_effect

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should be skipped (no non-empty items)
        assert values is None
        assert columns is None

    @patch('src.schemas.schemas.get_mongo_collection')
    @patch('src.schemas.schemas.connect_postgres')
    def test_extract_skips_no_matching_users_logbook(self, mock_pg_conn, mock_mongo_get, strategy, import_config):
        """Test that records without matching users_logbook are skipped"""
        userquizz_id = ObjectId()
        question_id = ObjectId()
        quizz_question_id = ObjectId()
        user_id = ObjectId()

        document = {
            '_id': userquizz_id,
            'name': 'Morning Check-in',
            'questions': [question_id],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        # Mock MongoDB collections - valid question
        mock_userquizzquestions = Mock()
        mock_userquizzquestions.find_one.return_value = {
            '_id': question_id,
            'quizzQuestion': quizz_question_id
        }

        mock_quizzquestions = Mock()
        mock_quizzquestions.find_one.return_value = {
            '_id': quizz_question_id,
            'type': 'QUIZZ_TYPE_LOGBOOK',
            'title': 'How are you feeling?'
        }

        mock_items = Mock()
        mock_items.count_documents.return_value = 1

        mock_coachinglogbooks = Mock()
        mock_coachinglogbooks.find_one.return_value = {
            'user': user_id,
            'day': datetime(2024, 1, 15).date()
        }

        def get_collection_side_effect(name):
            if name == 'userquizzquestions':
                return mock_userquizzquestions
            elif name == 'quizzquestions':
                return mock_quizzquestions
            elif name == 'items':
                return mock_items
            elif name == 'coachinglogbooks':
                return mock_coachinglogbooks
            return Mock()

        mock_mongo_get.side_effect = get_collection_side_effect

        # Mock PostgreSQL connection - no matching users_logbook
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None  # No match!
        mock_pg_conn.return_value.cursor.return_value = mock_cursor

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should be skipped (no matching users_logbook)
        assert values is None
        assert columns is None

    def test_extract_skips_empty_questions_array(self, strategy, import_config):
        """Test that userquizzs with no questions are skipped"""
        userquizz_id = ObjectId()

        document = {
            '_id': userquizz_id,
            'name': 'Empty Quiz',
            'questions': [],  # Empty!
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should be skipped
        assert values is None
        assert columns is None


class TestUsersLogbooksMomentsDetailsStrategy:
    """Test suite for UsersLogbooksMomentsDetailsStrategy"""

    @pytest.fixture
    def strategy(self):
        """Create a UsersLogbooksMomentsDetailsStrategy instance"""
        return _create_users_logbooks_moments_details_strategy()

    @pytest.fixture
    def mock_collection(self):
        """Create a mock MongoDB collection"""
        collection = Mock()
        collection.count_documents = Mock(return_value=1)
        collection.find = Mock()
        return collection

    @pytest.fixture
    def import_config(self):
        """Create a test ImportConfig"""
        return ImportConfig(
            table_name='users_logbooks_moments_details',
            source_collection='userquizzs',
            batch_size=5000,
            after_date=datetime(2024, 1, 1),
            summary_instance=Mock()
        )

    @patch('src.schemas.schemas.get_mongo_collection')
    def test_extract_with_logbook_questions_and_answers(self, mock_mongo_get, strategy, import_config):
        """Test extracting details with QUIZZ_TYPE_LOGBOOK questions and non-empty answers"""
        userquizz_id = ObjectId()
        question_id = ObjectId()
        quizz_question_id = ObjectId()
        item1_id = ObjectId()
        item2_id = ObjectId()

        document = {
            '_id': userquizz_id,
            'questions': [question_id]
        }

        # Mock collections
        mock_userquizzquestions = Mock()
        mock_userquizzquestions.find_one.return_value = {
            '_id': question_id,
            'quizzQuestion': quizz_question_id
        }

        mock_quizzquestions = Mock()
        mock_quizzquestions.find_one.return_value = {
            '_id': quizz_question_id,
            'type': 'QUIZZ_TYPE_LOGBOOK',
            'title': 'How are you feeling today?'
        }

        mock_items = Mock()
        mock_items.find.return_value = [
            {
                '_id': item1_id,
                'text': 'I feel great!',
                'creation_date': datetime(2024, 1, 15, 9, 0),
                'update_date': datetime(2024, 1, 15, 9, 5)
            },
            {
                '_id': item2_id,
                'text': 'Very energetic today',
                'creation_date': datetime(2024, 1, 15, 9, 1),
                'update_date': datetime(2024, 1, 15, 9, 6)
            }
        ]

        def get_collection_side_effect(name):
            if name == 'userquizzquestions':
                return mock_userquizzquestions
            elif name == 'quizzquestions':
                return mock_quizzquestions
            elif name == 'items':
                return mock_items
            return Mock()

        mock_mongo_get.side_effect = get_collection_side_effect

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify columns
        assert columns == ['id', 'user_logbook_moment_id', 'question', 'answer', 'created_at', 'updated_at']

        # Verify 2 detail rows (one per answer)
        assert len(values) == 2

        # Verify first row
        assert values[0][0] == str(item1_id)
        assert values[0][1] == str(userquizz_id)
        assert values[0][2] == 'How are you feeling today?'
        assert values[0][3] == 'I feel great!'
        assert values[0][4] == datetime(2024, 1, 15, 9, 0)
        assert values[0][5] == datetime(2024, 1, 15, 9, 5)

        # Verify second row
        assert values[1][0] == str(item2_id)
        assert values[1][1] == str(userquizz_id)
        assert values[1][2] == 'How are you feeling today?'
        assert values[1][3] == 'Very energetic today'

    @patch('src.schemas.schemas.get_mongo_collection')
    def test_extract_skips_non_logbook_questions(self, mock_mongo_get, strategy, import_config):
        """Test that non-LOGBOOK questions are filtered out"""
        userquizz_id = ObjectId()
        question_id = ObjectId()
        quizz_question_id = ObjectId()

        document = {
            '_id': userquizz_id,
            'questions': [question_id]
        }

        # Mock collections
        mock_userquizzquestions = Mock()
        mock_userquizzquestions.find_one.return_value = {
            '_id': question_id,
            'quizzQuestion': quizz_question_id
        }

        mock_quizzquestions = Mock()
        mock_quizzquestions.find_one.return_value = {
            '_id': quizz_question_id,
            'type': 'QUIZZ_TYPE_DIET',  # Wrong type!
            'title': 'What is your diet?'
        }

        def get_collection_side_effect(name):
            if name == 'userquizzquestions':
                return mock_userquizzquestions
            elif name == 'quizzquestions':
                return mock_quizzquestions
            return Mock()

        mock_mongo_get.side_effect = get_collection_side_effect

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should return empty batch (no valid questions)
        assert len(values) == 0

    @patch('src.schemas.schemas.get_mongo_collection')
    def test_extract_filters_empty_text_items(self, mock_mongo_get, strategy, import_config):
        """Test that items with empty text are filtered by MongoDB query"""
        userquizz_id = ObjectId()
        question_id = ObjectId()
        quizz_question_id = ObjectId()
        item_id = ObjectId()

        document = {
            '_id': userquizz_id,
            'questions': [question_id]
        }

        # Mock collections
        mock_userquizzquestions = Mock()
        mock_userquizzquestions.find_one.return_value = {
            '_id': question_id,
            'quizzQuestion': quizz_question_id
        }

        mock_quizzquestions = Mock()
        mock_quizzquestions.find_one.return_value = {
            '_id': quizz_question_id,
            'type': 'QUIZZ_TYPE_LOGBOOK',
            'title': 'How are you feeling?'
        }

        mock_items = Mock()
        # Only return non-empty items (MongoDB filtering)
        mock_items.find.return_value = [
            {
                '_id': item_id,
                'text': 'Good answer',
                'creation_date': datetime(2024, 1, 15),
                'update_date': datetime(2024, 1, 15)
            }
        ]

        def get_collection_side_effect(name):
            if name == 'userquizzquestions':
                return mock_userquizzquestions
            elif name == 'quizzquestions':
                return mock_quizzquestions
            elif name == 'items':
                return mock_items
            return Mock()

        mock_mongo_get.side_effect = get_collection_side_effect

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify that items.find was called with correct filter
        items_find_call = mock_items.find.call_args[0][0]
        assert 'userQuizzQuestion' in items_find_call
        assert items_find_call['text'] == {'$exists': True, '$ne': '', '$ne': None}

        # Should return 1 row
        assert len(values) == 1

    @patch('src.schemas.schemas.get_mongo_collection')
    def test_extract_handles_multiple_questions(self, mock_mongo_get, strategy, import_config):
        """Test extracting details from multiple questions"""
        userquizz_id = ObjectId()
        question1_id = ObjectId()
        question2_id = ObjectId()
        quizz_question1_id = ObjectId()
        quizz_question2_id = ObjectId()
        item1_id = ObjectId()
        item2_id = ObjectId()

        document = {
            '_id': userquizz_id,
            'questions': [question1_id, question2_id]
        }

        # Mock collections
        def userquizzquestion_side_effect(query):
            if query['_id'] == question1_id:
                return {'_id': question1_id, 'quizzQuestion': quizz_question1_id}
            elif query['_id'] == question2_id:
                return {'_id': question2_id, 'quizzQuestion': quizz_question2_id}
            return None

        mock_userquizzquestions = Mock()
        mock_userquizzquestions.find_one.side_effect = userquizzquestion_side_effect

        def quizzquestion_side_effect(query):
            if query['_id'] == quizz_question1_id:
                return {'_id': quizz_question1_id, 'type': 'QUIZZ_TYPE_LOGBOOK', 'title': 'Question 1'}
            elif query['_id'] == quizz_question2_id:
                return {'_id': quizz_question2_id, 'type': 'QUIZZ_TYPE_LOGBOOK', 'title': 'Question 2'}
            return None

        mock_quizzquestions = Mock()
        mock_quizzquestions.find_one.side_effect = quizzquestion_side_effect

        def items_find_side_effect(query):
            if query['userQuizzQuestion'] == question1_id:
                return [{'_id': item1_id, 'text': 'Answer 1', 'creation_date': datetime(2024, 1, 15), 'update_date': datetime(2024, 1, 15)}]
            elif query['userQuizzQuestion'] == question2_id:
                return [{'_id': item2_id, 'text': 'Answer 2', 'creation_date': datetime(2024, 1, 15), 'update_date': datetime(2024, 1, 15)}]
            return []

        mock_items = Mock()
        mock_items.find.side_effect = items_find_side_effect

        def get_collection_side_effect(name):
            if name == 'userquizzquestions':
                return mock_userquizzquestions
            elif name == 'quizzquestions':
                return mock_quizzquestions
            elif name == 'items':
                return mock_items
            return Mock()

        mock_mongo_get.side_effect = get_collection_side_effect

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should return 2 rows (one per question)
        assert len(values) == 2
        assert values[0][2] == 'Question 1'
        assert values[0][3] == 'Answer 1'
        assert values[1][2] == 'Question 2'
        assert values[1][3] == 'Answer 2'

    def test_extract_empty_questions_array(self, strategy, import_config):
        """Test handling userquizzs with no questions"""
        userquizz_id = ObjectId()

        document = {
            '_id': userquizz_id,
            'questions': []
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should return empty batch
        assert len(values) == 0
        assert columns == ['id', 'user_logbook_moment_id', 'question', 'answer', 'created_at', 'updated_at']

    def test_count_documents_with_date_filter(self, strategy, mock_collection, import_config):
        """Test counting documents with date filter"""
        strategy.count_total_documents(mock_collection, import_config)

        # Verify MongoDB query includes questions existence check
        call_args = mock_collection.count_documents.call_args[0][0]
        assert 'questions' in call_args
        assert call_args['questions'] == {'$exists': True, '$ne': []}

        # Verify date filter is included
        assert '$or' in call_args

    def test_get_documents_with_projection(self, strategy, mock_collection, import_config):
        """Test fetching documents with correct field projection"""
        mock_cursor = Mock()
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = []
        mock_collection.find.return_value = mock_cursor

        strategy.get_documents(mock_collection, import_config, offset=0)

        # Verify projection includes only necessary fields
        call_args = mock_collection.find.call_args[0]
        projection = call_args[1] if len(call_args) > 1 else {}

        assert '_id' in projection
        assert 'questions' in projection

    def test_strategy_uses_on_conflict(self, strategy):
        """Test that strategy uses ON CONFLICT for upserts"""
        assert strategy.get_use_on_conflict() is True

    def test_progress_message_format(self, strategy):
        """Test custom progress message formatting"""
        message = strategy.get_progress_message(
            processed=50,
            total=100,
            table_name='users_logbooks_moments_details',
            total_records=250
        )

        # Should include userquizz count and detail count
        assert '50' in message
        assert '100' in message
        assert '250' in message
        assert 'userquizzs' in message.lower()
        assert 'question-answer' in message.lower()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
