"""
Integration Tests for Database Migration

Tests end-to-end migration scenarios including:
- Full migrations
- Incremental migrations
- Delete-and-insert correctness
- Data consistency
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from bson import ObjectId
from datetime import datetime, timedelta

from src.migration.data_export import get_last_insert_date, export_table_data
from src.schemas.schemas import _create_user_events_strategy, _create_users_targets_strategy
from src.migration.import_strategies import ImportConfig, ImportUtils, DirectTranslationStrategy


class TestIncrementalMigration:
    """Test incremental migration behavior"""

    def test_get_last_insert_date_with_data(self):
        """Test retrieving last migration date from non-empty table"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        # Simulate a table with data
        last_date = datetime(2024, 1, 15, 10, 30, 0)
        mock_cursor.fetchone.return_value = (last_date,)

        result = get_last_insert_date(mock_conn, 'test_table')

        assert result == last_date
        # Verify correct SQL query was executed
        assert mock_cursor.execute.called
        sql = mock_cursor.execute.call_args[0][0]
        assert 'GREATEST' in sql
        assert 'MAX(created_at)' in sql
        assert 'MAX(updated_at)' in sql
        assert 'test_table' in sql

    def test_get_last_insert_date_empty_table(self):
        """Test retrieving last migration date from empty table"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        # Simulate empty table (returns 1900-01-01)
        mock_cursor.fetchone.return_value = (datetime(1900, 1, 1, 0, 0, 0),)

        result = get_last_insert_date(mock_conn, 'test_table')

        # Should return None for empty tables
        assert result is None

    def test_get_last_insert_date_null_result(self):
        """Test handling when query returns NULL"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.return_value = (None,)

        result = get_last_insert_date(mock_conn, 'test_table')

        assert result is None


class TestDeleteAndInsertCorrectness:
    """Test that delete-and-insert pattern correctly handles additions and removals"""

    def test_user_events_handles_event_removal(self):
        """Test that removing events from MongoDB array deletes them from PostgreSQL"""
        strategy = _create_user_events_strategy()
        user_id = ObjectId()
        event1 = ObjectId()
        event2 = ObjectId()

        # Scenario: User originally had 2 events, now has only 1
        # PostgreSQL should delete both old relationships and insert only the current one

        mock_collection = Mock()
        mock_cursor = Mock()

        # Setup: get_documents returns user with only 1 event
        def mock_find(*args, **kwargs):
            cursor = Mock()
            cursor.skip.return_value = cursor
            cursor.limit.return_value = [{
                '_id': user_id,
                'registered_events': [event1],  # Only event1 remains
                'creation_date': datetime.now(),
                'update_date': datetime.now()
            }]
            return cursor

        mock_collection.find = mock_find
        mock_collection.count_documents.return_value = 1

        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 2  # Simulates deleting 2 old relationships

        config = ImportConfig(
            table_name='user_events',
            source_collection='users',
            summary_instance=Mock()
        )

        with patch.object(ImportUtils, 'execute_batch', return_value=1) as mock_execute:
            strategy.export_data(mock_conn, mock_collection, config)

            # Verify DELETE was called
            assert mock_cursor.execute.called
            delete_sql = mock_cursor.execute.call_args_list[0][0][0]
            assert 'DELETE FROM user_events' in delete_sql

            # Verify INSERT was called with only 1 relationship
            batch_values = mock_execute.call_args[0][1]
            assert len(batch_values) == 1
            assert batch_values[0][1] == str(event1)

    def test_users_targets_handles_array_changes(self):
        """Test that changes to any target array trigger full refresh"""
        strategy = _create_users_targets_strategy()
        user_id = ObjectId()
        target1 = ObjectId()
        target2 = ObjectId()
        target3 = ObjectId()

        # Scenario: User modifies targets
        # Before: targets=[target1, target2], specificity_targets=[], health_targets=[]
        # After: targets=[target1], specificity_targets=[target3], health_targets=[]
        # All relationships should be deleted and re-inserted

        mock_collection = Mock()

        def mock_find(*args, **kwargs):
            cursor = Mock()
            cursor.skip.return_value = cursor
            cursor.limit.return_value = [{
                '_id': user_id,
                'targets': [target1],  # Removed target2
                'specificity_targets': [target3],  # Added target3
                'health_targets': [],
                'creation_date': datetime.now(),
                'update_date': datetime.now()
            }]
            return cursor

        mock_collection.find = mock_find
        mock_collection.count_documents.return_value = 1

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 2  # Simulates deleting old relationships

        config = ImportConfig(
            table_name='users_targets',
            source_collection='users',
            summary_instance=Mock()
        )

        with patch.object(ImportUtils, 'execute_batch', return_value=2) as mock_execute:
            strategy.export_data(mock_conn, mock_collection, config)

            # Verify DELETE was called for this user
            assert mock_cursor.execute.called
            delete_sql = mock_cursor.execute.call_args_list[0][0][0]
            assert 'DELETE FROM users_targets' in delete_sql

            # Verify INSERT was called with 2 relationships (1 basic, 1 specificity)
            batch_values = mock_execute.call_args[0][1]
            assert len(batch_values) == 2

            types = [row[2] for row in batch_values]
            assert 'basic' in types
            assert 'specificity' in types

            # Verify target2 is NOT in the insert (was removed)
            target_ids = [row[1] for row in batch_values]
            assert str(target1) in target_ids
            assert str(target3) in target_ids
            assert str(target2) not in target_ids


class TestDataConsistency:
    """Test that data remains consistent across migrations"""

    def test_incremental_sync_does_not_duplicate_data(self):
        """Test that incremental sync with unchanged data doesn't duplicate records"""
        # This tests DirectTranslationStrategy with ON CONFLICT DO UPDATE

        strategy = DirectTranslationStrategy()

        user_id = ObjectId()
        last_migration = datetime(2024, 1, 1)
        current_time = datetime(2024, 1, 15)

        mock_collection = Mock()

        def mock_find(*args, **kwargs):
            # Return same user document (simulating no changes)
            return [{
                '_id': user_id,
                'name': 'Test User',
                'email': 'test@example.com',
                'creation_date': datetime(2023, 12, 1),  # Before last migration
                'update_date': current_time  # After last migration (triggers re-fetch)
            }]

        mock_collection.find.return_value = mock_find()
        mock_collection.count_documents.return_value = 1

        mock_conn = Mock()

        config = ImportConfig(
            table_name='users',
            source_collection='users',
            after_date=last_migration,
            summary_instance=Mock()
        )

        with patch.object(ImportUtils, 'execute_batch', return_value=1) as mock_execute:
            strategy.export_data(mock_conn, mock_collection, config)

            # Verify ON CONFLICT DO UPDATE was used
            call_args = mock_execute.call_args
            assert call_args[1]['use_on_conflict'] is True

            # Record should be updated, not duplicated
            batch_values = call_args[0][1]
            assert len(batch_values) == 1

    def test_delete_and_insert_maintains_referential_integrity(self):
        """Test that delete-and-insert maintains referential integrity"""
        strategy = _create_user_events_strategy()
        user_id = ObjectId()
        valid_event = ObjectId()

        mock_collection = Mock()

        def mock_find(*args, **kwargs):
            cursor = Mock()
            cursor.skip.return_value = cursor
            cursor.limit.return_value = [{
                '_id': user_id,
                'registered_events': [valid_event],
                'creation_date': datetime.now(),
                'update_date': datetime.now()
            }]
            return cursor

        mock_collection.find = mock_find
        mock_collection.count_documents.return_value = 1

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        config = ImportConfig(
            table_name='user_events',
            source_collection='users',
            summary_instance=Mock()
        )

        with patch.object(ImportUtils, 'execute_batch', return_value=1) as mock_execute:
            strategy.export_data(mock_conn, mock_collection, config)

            # Verify no ON CONFLICT clause (delete-and-insert pattern)
            call_args = mock_execute.call_args
            assert call_args[1]['use_on_conflict'] is False
            assert call_args[1]['on_conflict_clause'] == ""

            # All inserts should be fresh (no conflicts expected)
            batch_values = call_args[0][1]
            assert len(batch_values) == 1
            assert batch_values[0][0] == str(user_id)
            assert batch_values[0][1] == str(valid_event)


class TestBatchProcessing:
    """Test batch processing behavior"""

    def test_handles_large_batches(self):
        """Test that large batches are processed correctly"""
        strategy = _create_user_events_strategy()

        # Create 100 users with events
        users = []
        for i in range(100):
            users.append({
                '_id': ObjectId(),
                'registered_events': [ObjectId(), ObjectId()],
                'creation_date': datetime.now(),
                'update_date': datetime.now()
            })

        mock_collection = Mock()

        def mock_find(*args, **kwargs):
            cursor = Mock()
            cursor.skip.return_value = cursor
            cursor.limit.return_value = users
            return cursor

        mock_collection.find = mock_find
        mock_collection.count_documents.return_value = 100

        mock_conn = Mock()
        mock_conn.cursor.return_value = Mock()

        config = ImportConfig(
            table_name='user_events',
            source_collection='users',
            batch_size=5000,
            summary_instance=Mock()
        )

        with patch.object(ImportUtils, 'execute_batch', return_value=200) as mock_execute:
            result = strategy.export_data(mock_conn, mock_collection, config)

            # Verify all 200 relationships were inserted (100 users × 2 events)
            batch_values = mock_execute.call_args[0][1]
            assert len(batch_values) == 200


class TestErrorRecovery:
    """Test error handling and recovery"""

    def test_handles_delete_error_gracefully(self):
        """Test that errors during DELETE don't prevent INSERT"""
        strategy = _create_user_events_strategy()
        user_id = ObjectId()
        event_id = ObjectId()

        mock_collection = Mock()

        def mock_find(*args, **kwargs):
            cursor = Mock()
            cursor.skip.return_value = cursor
            cursor.limit.return_value = [{
                '_id': user_id,
                'registered_events': [event_id],
                'creation_date': datetime.now(),
                'update_date': datetime.now()
            }]
            return cursor

        mock_collection.find = mock_find
        mock_collection.count_documents.return_value = 1

        # Setup mock connection to raise error on DELETE
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database error")
        mock_conn.cursor.return_value = mock_cursor

        config = ImportConfig(
            table_name='user_events',
            source_collection='users',
            summary_instance=Mock()
        )

        with patch.object(ImportUtils, 'execute_batch', return_value=1) as mock_execute:
            # Should not raise exception
            result = strategy.export_data(mock_conn, mock_collection, config)

            # Verify rollback was called
            assert mock_conn.rollback.called

            # INSERT should still be attempted
            assert mock_execute.called


class TestMigrationPerformance:
    """Test performance-related aspects"""

    def test_uses_pagination_for_large_datasets(self):
        """Test that pagination is used correctly for large datasets"""
        strategy = _create_user_events_strategy()

        call_count = [0]

        def mock_find(*args, **kwargs):
            call_count[0] += 1
            cursor = Mock()
            cursor.skip.return_value = cursor

            # First call returns full batch, second returns empty
            if call_count[0] == 1:
                cursor.limit.return_value = [
                    {
                        '_id': ObjectId(),
                        'registered_events': [ObjectId()],
                        'creation_date': datetime.now(),
                        'update_date': datetime.now()
                    }
                    for _ in range(5000)  # Full batch
                ]
            else:
                cursor.limit.return_value = []  # No more data

            return cursor

        mock_collection = Mock()
        mock_collection.find = mock_find
        mock_collection.count_documents.return_value = 5000

        mock_conn = Mock()
        mock_conn.cursor.return_value = Mock()

        config = ImportConfig(
            table_name='user_events',
            source_collection='users',
            batch_size=5000,
            summary_instance=Mock()
        )

        with patch.object(ImportUtils, 'execute_batch', return_value=5000):
            strategy.export_data(mock_conn, mock_collection, config)

            # Verify find was called twice (pagination)
            assert call_count[0] == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
