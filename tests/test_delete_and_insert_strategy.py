"""
Tests for DeleteAndInsertStrategy base class

Tests the template method pattern and delete-and-insert logic
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from bson import ObjectId
from datetime import datetime

# Import the strategies
from src.migration.import_strategies import DeleteAndInsertStrategy, ImportConfig, ImportUtils


class MockDeleteAndInsertStrategy(DeleteAndInsertStrategy):
    """Concrete implementation for testing the abstract base class"""

    def __init__(self):
        self.parent_ids_extracted = []
        self.documents_processed = []

    def count_total_documents(self, collection, config: ImportConfig) -> int:
        """Mock implementation returning fixed count"""
        return 3

    def get_documents(self, collection, config: ImportConfig, offset: int = 0):
        """Mock implementation returning test documents"""
        if offset == 0:
            # First batch
            return [
                {'_id': ObjectId(), 'name': 'doc1', 'items': ['a', 'b']},
                {'_id': ObjectId(), 'name': 'doc2', 'items': ['c']},
            ]
        else:
            # No more documents
            return []

    def extract_data_for_sql(self, document, config: ImportConfig):
        """Mock implementation extracting data from document"""
        self.documents_processed.append(document)
        doc_id = str(document['_id'])
        items = document.get('items', [])

        batch_values = []
        for item in items:
            batch_values.append([doc_id, item, datetime.now(), datetime.now()])

        return batch_values, ['parent_id', 'item', 'created_at', 'updated_at']

    def get_parent_id_from_document(self, document) -> str:
        """Mock implementation extracting parent ID"""
        parent_id = str(document['_id'])
        self.parent_ids_extracted.append(parent_id)
        return parent_id

    def get_delete_table_name(self, config: ImportConfig) -> str:
        """Mock implementation returning table name"""
        return config.table_name

    def get_delete_column_name(self) -> str:
        """Mock implementation returning column name"""
        return 'parent_id'


class TestDeleteAndInsertStrategy:
    """Test suite for DeleteAndInsertStrategy base class"""

    @pytest.fixture
    def mock_conn(self):
        """Create a mock database connection"""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value = cursor
        cursor.rowcount = 5  # Simulate 5 rows deleted
        return conn

    @pytest.fixture
    def mock_collection(self):
        """Create a mock MongoDB collection"""
        return Mock()

    @pytest.fixture
    def import_config(self):
        """Create a test ImportConfig"""
        return ImportConfig(
            table_name='test_relationships',
            source_collection='test_collection',
            batch_size=5000,
            after_date=None,
            summary_instance=Mock()
        )

    @pytest.fixture
    def strategy(self):
        """Create a MockDeleteAndInsertStrategy instance"""
        return MockDeleteAndInsertStrategy()

    def test_template_method_flow(self, strategy, mock_conn, mock_collection, import_config):
        """Test that the template method calls all required methods in correct order"""
        with patch.object(ImportUtils, 'execute_batch', return_value=3) as mock_execute:
            result = strategy.export_data(mock_conn, mock_collection, import_config)

            # Verify documents were processed
            assert len(strategy.documents_processed) == 2
            assert strategy.documents_processed[0]['name'] == 'doc1'
            assert strategy.documents_processed[1]['name'] == 'doc2'

            # Verify parent IDs were extracted
            assert len(strategy.parent_ids_extracted) == 2

            # Verify execute_batch was called
            assert mock_execute.called

    def test_delete_existing_relationships(self, strategy, mock_conn, mock_collection, import_config):
        """Test that DELETE query is executed correctly for changed parents"""
        with patch.object(ImportUtils, 'execute_batch', return_value=3):
            strategy.export_data(mock_conn, mock_collection, import_config)

            # Verify cursor was created and DELETE was executed
            mock_conn.cursor.assert_called()
            cursor = mock_conn.cursor.return_value

            # Check that execute was called with DELETE statement
            assert cursor.execute.called
            delete_call = cursor.execute.call_args_list[0]
            sql = delete_call[0][0]
            assert 'DELETE FROM test_relationships' in sql
            assert 'WHERE parent_id IN' in sql

            # Verify commit was called
            mock_conn.commit.assert_called()

            # Verify cursor was closed
            cursor.close.assert_called()

    def test_insert_fresh_relationships(self, strategy, mock_conn, mock_collection, import_config):
        """Test that INSERT is executed with correct data"""
        with patch.object(ImportUtils, 'execute_batch', return_value=3) as mock_execute:
            strategy.export_data(mock_conn, mock_collection, import_config)

            # Verify execute_batch was called
            assert mock_execute.called

            # Check parameters passed to execute_batch
            call_args = mock_execute.call_args
            batch_values = call_args[0][1]
            columns = call_args[0][2]
            table_name = call_args[0][3]

            # Verify table name
            assert table_name == 'test_relationships'

            # Verify columns
            assert columns == ['parent_id', 'item', 'created_at', 'updated_at']

            # Verify batch values contain data from both documents
            assert len(batch_values) == 3  # doc1 has 2 items, doc2 has 1 item

            # Verify use_on_conflict is False (delete-and-insert pattern)
            assert call_args[1]['use_on_conflict'] is False
            assert call_args[1]['on_conflict_clause'] == ""

    def test_batch_processing_pagination(self, strategy, mock_conn, mock_collection, import_config):
        """Test that batching and pagination work correctly"""
        with patch.object(ImportUtils, 'execute_batch', return_value=3):
            strategy.export_data(mock_conn, mock_collection, import_config)

            # Verify get_documents was called with correct offsets
            # First call with offset=0 returns 2 documents
            # Second call with offset=5000 returns empty list (stops)
            assert len(strategy.documents_processed) == 2

    def test_error_handling_during_delete(self, strategy, mock_conn, mock_collection, import_config):
        """Test that errors during DELETE are handled gracefully"""
        # Make DELETE raise an exception
        cursor = mock_conn.cursor.return_value
        cursor.execute.side_effect = Exception("Database error")

        with patch.object(ImportUtils, 'execute_batch', return_value=3):
            # Should not raise, but handle error gracefully
            result = strategy.export_data(mock_conn, mock_collection, import_config)

            # Verify rollback was called
            mock_conn.rollback.assert_called()

            # Verify cursor was still closed
            cursor.close.assert_called()

    def test_no_on_conflict_for_delete_and_insert(self, strategy):
        """Test that get_use_on_conflict returns False for delete-and-insert pattern"""
        assert strategy.get_use_on_conflict() is False

    def test_empty_document_batch(self, strategy, mock_conn, mock_collection, import_config):
        """Test handling when no documents are returned"""
        # Override get_documents to return empty list
        strategy.get_documents = lambda coll, conf, offset: []

        with patch.object(ImportUtils, 'execute_batch') as mock_execute:
            result = strategy.export_data(mock_conn, mock_collection, import_config)

            # execute_batch should not be called if no documents
            assert not mock_execute.called

            # No DELETE should occur
            assert not mock_conn.cursor.called

    def test_progress_message_default(self, strategy):
        """Test default progress message format"""
        message = strategy.get_progress_message(50, 100, 'test_table', total_records=150)

        # Should contain processed count and table name
        assert '50' in message
        assert '100' in message
        assert 'test_table' in message

    def test_handles_multiple_batches(self, strategy, mock_conn, mock_collection, import_config):
        """Test that multiple batches are processed correctly"""
        # Override get_documents to return multiple batches
        call_count = [0]

        def get_docs_multi_batch(coll, conf, offset):
            call_count[0] += 1
            if call_count[0] == 1:
                return [{'_id': ObjectId(), 'items': ['a']}, {'_id': ObjectId(), 'items': ['b']}]
            elif call_count[0] == 2:
                return [{'_id': ObjectId(), 'items': ['c']}]
            else:
                return []

        strategy.get_documents = get_docs_multi_batch

        with patch.object(ImportUtils, 'execute_batch', return_value=2) as mock_execute:
            result = strategy.export_data(mock_conn, mock_collection, import_config)

            # execute_batch should be called twice (once per batch)
            assert mock_execute.call_count == 2

            # DELETE should be called twice (once per batch)
            cursor = mock_conn.cursor.return_value
            assert cursor.execute.call_count == 2


class TestDeleteAndInsertIntegration:
    """Integration tests for DeleteAndInsertStrategy with real-like scenarios"""

    def test_user_events_scenario(self):
        """Test a realistic user events migration scenario"""
        # Simulate user documents with registered_events arrays
        user1_id = ObjectId()
        user2_id = ObjectId()

        class UserEventsTestStrategy(DeleteAndInsertStrategy):
            def count_total_documents(self, collection, config):
                return 2

            def get_documents(self, collection, config, offset=0):
                if offset == 0:
                    return [
                        {
                            '_id': user1_id,
                            'registered_events': [ObjectId(), ObjectId()],
                            'creation_date': datetime.now()
                        },
                        {
                            '_id': user2_id,
                            'registered_events': [ObjectId()],
                            'creation_date': datetime.now()
                        }
                    ]
                return []

            def extract_data_for_sql(self, document, config):
                user_id = str(document['_id'])
                batch = []
                for event_id in document['registered_events']:
                    batch.append([user_id, str(event_id), datetime.now(), datetime.now()])
                return batch, ['user_id', 'event_id', 'created_at', 'updated_at']

            def get_parent_id_from_document(self, document):
                return str(document['_id'])

            def get_delete_table_name(self, config):
                return 'user_events'

            def get_delete_column_name(self):
                return 'user_id'

        strategy = UserEventsTestStrategy()
        mock_conn = Mock()
        mock_collection = Mock()
        config = ImportConfig('user_events', 'users', summary_instance=Mock())

        with patch.object(ImportUtils, 'execute_batch', return_value=3):
            result = strategy.export_data(mock_conn, mock_collection, config)

            # Verify DELETE was called for user_events table
            cursor = mock_conn.cursor.return_value
            delete_sql = cursor.execute.call_args_list[0][0][0]
            assert 'DELETE FROM user_events' in delete_sql
            assert 'user_id IN' in delete_sql

    def test_users_targets_scenario(self):
        """Test a realistic users_targets migration scenario with type discrimination"""
        user_id = ObjectId()

        class UsersTargetsTestStrategy(DeleteAndInsertStrategy):
            def count_total_documents(self, collection, config):
                return 1

            def get_documents(self, collection, config, offset=0):
                if offset == 0:
                    return [{
                        '_id': user_id,
                        'targets': [ObjectId(), ObjectId()],
                        'specificity_targets': [ObjectId()],
                        'health_targets': [ObjectId()],
                        'creation_date': datetime.now()
                    }]
                return []

            def extract_data_for_sql(self, document, config):
                uid = str(document['_id'])
                batch = []
                now = datetime.now()

                for tid in document.get('targets', []):
                    batch.append([uid, str(tid), 'basic', now, now])
                for tid in document.get('specificity_targets', []):
                    batch.append([uid, str(tid), 'specificity', now, now])
                for tid in document.get('health_targets', []):
                    batch.append([uid, str(tid), 'health', now, now])

                return batch, ['user_id', 'target_id', 'type', 'created_at', 'updated_at']

            def get_parent_id_from_document(self, document):
                return str(document['_id'])

            def get_delete_table_name(self, config):
                return 'users_targets'

            def get_delete_column_name(self):
                return 'user_id'

        strategy = UsersTargetsTestStrategy()
        mock_conn = Mock()
        mock_collection = Mock()
        config = ImportConfig('users_targets', 'users', summary_instance=Mock())

        with patch.object(ImportUtils, 'execute_batch', return_value=4) as mock_execute:
            result = strategy.export_data(mock_conn, mock_collection, config)

            # Verify data was inserted with correct type discrimination
            batch_values = mock_execute.call_args[0][1]
            assert len(batch_values) == 4  # 2 basic + 1 specificity + 1 health

            # Verify types are correct
            types = [row[2] for row in batch_values]
            assert types.count('basic') == 2
            assert types.count('specificity') == 1
            assert types.count('health') == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
