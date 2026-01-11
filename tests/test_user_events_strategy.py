"""
Tests for UserEventsStrategy

Tests the refactored UserEventsStrategy that extends DeleteAndInsertStrategy
"""

import pytest
from unittest.mock import Mock, patch
from bson import ObjectId
from datetime import datetime

from src.schemas.schemas import _create_user_events_strategy
from src.migration.import_strategies import ImportConfig, ImportUtils


class TestUserEventsStrategy:
    """Test suite for UserEventsStrategy"""

    @pytest.fixture
    def strategy(self):
        """Create a UserEventsStrategy instance"""
        return _create_user_events_strategy()

    @pytest.fixture
    def mock_collection(self):
        """Create a mock MongoDB collection"""
        collection = Mock()
        collection.count_documents = Mock(return_value=2)
        collection.find = Mock()
        return collection

    @pytest.fixture
    def import_config(self):
        """Create a test ImportConfig"""
        return ImportConfig(
            table_name='user_events',
            source_collection='users',
            batch_size=5000,
            after_date=datetime(2024, 1, 1),
            summary_instance=Mock()
        )

    def test_count_total_documents_with_filter(self, strategy, mock_collection, import_config):
        """Test counting documents with registered_events array"""
        strategy.count_total_documents(mock_collection, import_config)

        # Verify MongoDB query includes registered_events existence check
        call_args = mock_collection.count_documents.call_args[0][0]
        assert 'registered_events' in call_args
        assert call_args['registered_events'] == {'$exists': True, '$ne': []}

        # Verify date filter is included
        assert '$or' in call_args

    def test_count_total_documents_without_date_filter(self, strategy, mock_collection):
        """Test counting documents without after_date filter"""
        config = ImportConfig(
            table_name='user_events',
            source_collection='users',
            after_date=None,
            summary_instance=Mock()
        )

        strategy.count_total_documents(mock_collection, config)

        # Verify only registered_events filter is present
        call_args = mock_collection.count_documents.call_args[0][0]
        assert 'registered_events' in call_args
        # Without after_date, the filter should not contain $or
        assert '$or' not in call_args or call_args.get('$or') is None

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
        assert 'registered_events' in projection
        assert 'creation_date' in projection
        assert 'update_date' in projection

    def test_extract_data_for_objectid_array(self, strategy, import_config):
        """Test extracting data when registered_events contains ObjectIds"""
        user_id = ObjectId()
        event_id_1 = ObjectId()
        event_id_2 = ObjectId()

        document = {
            '_id': user_id,
            'registered_events': [event_id_1, event_id_2],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify columns
        assert columns == ['user_id', 'event_id', 'created_at', 'updated_at']

        # Verify we got 2 rows (one per event)
        assert len(values) == 2

        # Verify first row
        assert values[0][0] == str(user_id)  # user_id
        assert values[0][1] == str(event_id_1)  # event_id
        assert isinstance(values[0][2], datetime)  # created_at
        assert isinstance(values[0][3], datetime)  # updated_at

        # Verify second row
        assert values[1][0] == str(user_id)  # user_id
        assert values[1][1] == str(event_id_2)  # event_id

    def test_extract_data_for_embedded_documents(self, strategy, import_config):
        """Test extracting data when registered_events contains embedded documents"""
        user_id = ObjectId()
        event_id = ObjectId()

        document = {
            '_id': user_id,
            'registered_events': [
                {
                    'event': event_id,
                    'date': datetime(2024, 2, 1)
                }
            ],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify we got 1 row
        assert len(values) == 1

        # Verify the event_id was extracted from embedded document
        assert values[0][1] == str(event_id)

        # Verify the date was used from the embedded document
        assert values[0][2] == datetime(2024, 2, 1)

    def test_extract_data_for_mixed_formats(self, strategy, import_config):
        """Test extracting data when registered_events has mixed ObjectId and embedded docs"""
        user_id = ObjectId()
        event_id_1 = ObjectId()
        event_id_2 = ObjectId()

        document = {
            '_id': user_id,
            'registered_events': [
                event_id_1,  # Plain ObjectId
                {
                    'event': event_id_2,
                    'date': datetime(2024, 2, 1)
                }  # Embedded document
            ],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify we got 2 rows
        assert len(values) == 2

        # First row uses creation_date (plain ObjectId)
        assert values[0][1] == str(event_id_1)
        assert values[0][2] == datetime(2024, 1, 15)

        # Second row uses embedded document date
        assert values[1][1] == str(event_id_2)
        assert values[1][2] == datetime(2024, 2, 1)

    def test_extract_data_empty_events_array(self, strategy, import_config):
        """Test extracting data when registered_events is empty"""
        user_id = ObjectId()

        document = {
            '_id': user_id,
            'registered_events': [],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should return empty batch
        assert len(values) == 0

    def test_get_parent_id_from_document(self, strategy):
        """Test extracting user_id from document"""
        user_id = ObjectId()
        document = {'_id': user_id, 'name': 'Test User'}

        parent_id = strategy.get_parent_id_from_document(document)

        assert parent_id == str(user_id)

    def test_get_delete_table_name(self, strategy, import_config):
        """Test that correct table name is returned for deletion"""
        table_name = strategy.get_delete_table_name(import_config)

        assert table_name == 'user_events'

    def test_get_delete_column_name(self, strategy):
        """Test that correct column name is returned for WHERE clause"""
        column_name = strategy.get_delete_column_name()

        assert column_name == 'user_id'

    def test_progress_message_format(self, strategy):
        """Test custom progress message formatting"""
        message = strategy.get_progress_message(
            processed=50,
            total=100,
            table_name='user_events',
            total_records=150
        )

        # Should include user count and relationship count
        assert '50' in message
        assert '100' in message
        assert '150' in message
        assert 'users' in message.lower()
        assert 'user-event' in message.lower()

    def test_inherits_from_delete_and_insert_strategy(self, strategy):
        """Test that UserEventsStrategy correctly extends DeleteAndInsertStrategy"""
        from src.migration.import_strategies import DeleteAndInsertStrategy

        assert isinstance(strategy, DeleteAndInsertStrategy)

    def test_does_not_use_on_conflict(self, strategy):
        """Test that strategy uses delete-and-insert instead of ON CONFLICT"""
        # DeleteAndInsertStrategy should return False
        assert strategy.get_use_on_conflict() is False

    def test_handles_missing_dates(self, strategy, import_config):
        """Test handling documents with missing creation/update dates"""
        user_id = ObjectId()
        event_id = ObjectId()

        document = {
            '_id': user_id,
            'registered_events': [event_id],
            # No creation_date or update_date
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should still create row, but with None dates
        assert len(values) == 1
        assert values[0][0] == str(user_id)
        assert values[0][1] == str(event_id)
        # created_at and updated_at will be None or fallback values

    def test_full_export_cycle(self, strategy, import_config):
        """Integration test: full export cycle with mocked database"""
        user1_id = ObjectId()
        user2_id = ObjectId()
        event1_id = ObjectId()
        event2_id = ObjectId()

        mock_collection = Mock()
        mock_collection.count_documents.return_value = 2

        # Mock find to return test documents
        mock_cursor = Mock()
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = [
            {
                '_id': user1_id,
                'registered_events': [event1_id, event2_id],
                'creation_date': datetime(2024, 1, 15),
                'update_date': datetime(2024, 1, 20)
            },
            {
                '_id': user2_id,
                'registered_events': [event1_id],
                'creation_date': datetime(2024, 1, 10),
                'update_date': datetime(2024, 1, 18)
            }
        ]
        mock_collection.find.return_value = mock_cursor

        mock_conn = Mock()
        mock_conn.cursor.return_value = Mock()

        with patch.object(ImportUtils, 'execute_batch', return_value=3) as mock_execute:
            result = strategy.export_data(mock_conn, mock_collection, import_config)

            # Verify execute_batch was called
            assert mock_execute.called

            # Verify correct number of relationships inserted (2 + 1 = 3)
            batch_values = mock_execute.call_args[0][1]
            assert len(batch_values) == 3

            # Verify user1 has 2 events
            user1_events = [v for v in batch_values if v[0] == str(user1_id)]
            assert len(user1_events) == 2

            # Verify user2 has 1 event
            user2_events = [v for v in batch_values if v[0] == str(user2_id)]
            assert len(user2_events) == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
