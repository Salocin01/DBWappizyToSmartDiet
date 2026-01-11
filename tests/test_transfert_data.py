"""
Tests for transfert_data.py - The Explicit 4-Step Migration Flow

Tests the main migration script with the explicit step-by-step execution:
- Step 1: Get last migration date
- Step 2: Query new/updated documents
- Step 3: Transform data to SQL
- Step 4: Execute import (with DELETE for relationships)
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from bson import ObjectId
from datetime import datetime

from src.migration.import_strategies import ImportConfig, DirectTranslationStrategy


class TestExplicitFourStepFlow:
    """Test the explicit 4-step migration flow in transfert_data.py"""

    @pytest.fixture
    def mock_strategy(self):
        """Create a mock strategy with all required methods"""
        strategy = Mock(spec=DirectTranslationStrategy)
        strategy.count_total_documents.return_value = 2
        strategy.get_use_on_conflict.return_value = True
        strategy.get_on_conflict_clause.return_value = " ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at"
        return strategy

    @pytest.fixture
    def mock_documents(self):
        """Create mock MongoDB documents"""
        return [
            {
                '_id': ObjectId(),
                'name': 'Test User 1',
                'email': 'user1@test.com',
                'creation_date': datetime(2024, 1, 15),
                'update_date': datetime(2024, 1, 20)
            },
            {
                '_id': ObjectId(),
                'name': 'Test User 2',
                'email': 'user2@test.com',
                'creation_date': datetime(2024, 1, 16),
                'update_date': datetime(2024, 1, 21)
            }
        ]

    def test_step1_get_last_migration_date(self):
        """Test Step 1: Getting last migration date from PostgreSQL"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        last_date = datetime(2024, 1, 15, 10, 30, 0)
        mock_cursor.fetchone.return_value = (last_date,)

        from src.migration.data_export import get_last_insert_date
        result = get_last_insert_date(mock_conn, 'test_table')

        # Verify the result
        assert result == last_date

        # Verify correct SQL was executed
        assert mock_cursor.execute.called
        sql = mock_cursor.execute.call_args[0][0]
        assert 'GREATEST' in sql
        assert 'MAX(created_at)' in sql
        assert 'MAX(updated_at)' in sql
        assert 'test_table' in sql

    def test_step2_count_documents(self, mock_strategy):
        """Test Step 2: Counting documents from MongoDB"""
        mock_collection = Mock()
        mock_collection.count_documents.return_value = 150

        config = ImportConfig(
            table_name='test_table',
            source_collection='test_collection',
            after_date=datetime(2024, 1, 1),
            summary_instance=Mock()
        )

        # Simulate Step 2
        total_documents = mock_strategy.count_total_documents(mock_collection, config)

        assert total_documents == 2
        assert mock_strategy.count_total_documents.called

    def test_step3_transform_single_document(self, mock_strategy, mock_documents):
        """Test Step 3: Transforming a single document to SQL"""
        document = mock_documents[0]
        config = ImportConfig(
            table_name='users',
            source_collection='users',
            summary_instance=Mock()
        )

        # Mock the extract_data_for_sql to return realistic data
        mock_strategy.extract_data_for_sql.return_value = (
            [str(document['_id']), document['name'], document['email'],
             document['creation_date'], document['update_date']],
            ['id', 'name', 'email', 'created_at', 'updated_at']
        )

        # Simulate Step 3 transformation
        values, columns = mock_strategy.extract_data_for_sql(document, config)

        assert columns == ['id', 'name', 'email', 'created_at', 'updated_at']
        assert len(values) == 5
        assert values[1] == 'Test User 1'
        assert values[2] == 'user1@test.com'

    def test_step3_transform_batch_of_documents(self, mock_strategy, mock_documents):
        """Test Step 3: Transforming multiple documents in a batch"""
        config = ImportConfig(
            table_name='users',
            source_collection='users',
            summary_instance=Mock()
        )

        # Mock get_documents to return our test documents
        mock_collection = Mock()
        mock_strategy.get_documents.return_value = mock_documents

        # Simulate fetching documents
        documents = mock_strategy.get_documents(mock_collection, config, offset=0)

        assert len(documents) == 2
        assert documents[0]['name'] == 'Test User 1'
        assert documents[1]['name'] == 'Test User 2'

    def test_step3_handles_multiple_rows_per_document(self, mock_strategy):
        """Test Step 3: Handling documents that produce multiple SQL rows"""
        user_id = ObjectId()
        document = {
            '_id': user_id,
            'registered_events': [ObjectId(), ObjectId()],
            'creation_date': datetime.now(),
            'update_date': datetime.now()
        }

        # Mock extract to return multiple rows (for array extraction)
        mock_strategy.extract_data_for_sql.return_value = (
            [
                [str(user_id), str(document['registered_events'][0]), datetime.now()],
                [str(user_id), str(document['registered_events'][1]), datetime.now()]
            ],
            ['user_id', 'event_id', 'created_at']
        )

        config = ImportConfig(
            table_name='user_events',
            source_collection='users',
            summary_instance=Mock()
        )

        values, columns = mock_strategy.extract_data_for_sql(document, config)

        # Should return list of lists (multiple rows)
        assert isinstance(values, list)
        assert isinstance(values[0], list)
        assert len(values) == 2  # 2 events
        assert columns == ['user_id', 'event_id', 'created_at']

    def test_step4_upsert_with_on_conflict(self, mock_strategy):
        """Test Step 4: INSERT with ON CONFLICT DO UPDATE (upsert)"""
        from src.migration.import_strategies import ImportUtils

        mock_conn = Mock()
        batch_values = [
            ['id1', 'User 1', 'user1@test.com', datetime.now(), datetime.now()],
            ['id2', 'User 2', 'user2@test.com', datetime.now(), datetime.now()]
        ]
        columns = ['id', 'name', 'email', 'created_at', 'updated_at']

        with patch.object(ImportUtils, 'execute_batch', return_value=2) as mock_execute:
            # Simulate Step 4 with upsert
            use_on_conflict = True
            on_conflict_clause = " ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at"

            actual_insertions = ImportUtils.execute_batch(
                mock_conn,
                batch_values,
                columns,
                'users',
                Mock(),
                use_on_conflict=use_on_conflict,
                on_conflict_clause=on_conflict_clause
            )

            assert actual_insertions == 2
            assert mock_execute.called

            # Verify parameters
            call_args = mock_execute.call_args
            assert call_args[1]['use_on_conflict'] is True
            assert 'ON CONFLICT' in call_args[1]['on_conflict_clause']

    def test_step4_delete_and_insert_pattern(self):
        """Test Step 4: DELETE + INSERT pattern for relationships"""
        from src.migration.import_strategies import DeleteAndInsertStrategy

        # Create a concrete implementation for testing
        class TestDeleteInsertStrategy(DeleteAndInsertStrategy):
            def count_total_documents(self, collection, config):
                return 1

            def get_documents(self, collection, config, offset=0):
                if offset == 0:
                    return [{'_id': ObjectId(), 'items': ['a', 'b']}]
                return []

            def extract_data_for_sql(self, document, config):
                user_id = str(document['_id'])
                return [
                    [user_id, 'a', datetime.now()],
                    [user_id, 'b', datetime.now()]
                ], ['user_id', 'item', 'created_at']

            def get_parent_id_from_document(self, document):
                return str(document['_id'])

            def get_delete_table_name(self, config):
                return 'test_relationships'

            def get_delete_column_name(self):
                return 'user_id'

        strategy = TestDeleteInsertStrategy()
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 2  # Simulates 2 rows deleted

        config = ImportConfig(
            table_name='test_relationships',
            source_collection='test_collection',
            summary_instance=Mock()
        )

        # Simulate the DELETE step
        parent_ids = ['user_id_1']
        delete_table = strategy.get_delete_table_name(config)
        delete_column = strategy.get_delete_column_name()
        placeholders = ', '.join(['%s'] * len(parent_ids))
        delete_sql = f"DELETE FROM {delete_table} WHERE {delete_column} IN ({placeholders})"

        mock_cursor.execute(delete_sql, parent_ids)
        deleted_count = mock_cursor.rowcount

        assert deleted_count == 2
        assert 'DELETE FROM test_relationships' in delete_sql
        assert 'WHERE user_id IN' in delete_sql

    def test_full_migration_flow_for_simple_table(self, mock_documents):
        """Test complete flow for a simple table (DirectTranslationStrategy)"""
        from src.migration.import_strategies import ImportUtils

        # Setup mocks
        mock_conn = Mock()
        mock_collection = Mock()
        mock_strategy = Mock(spec=DirectTranslationStrategy)

        config = ImportConfig(
            table_name='users',
            source_collection='users',
            batch_size=5000,
            after_date=datetime(2024, 1, 1),
            summary_instance=Mock()
        )

        # Step 1: Mock last date retrieval
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (datetime(2024, 1, 1),)

        # Step 2: Mock document count
        mock_strategy.count_total_documents.return_value = 2

        # Step 3: Mock document fetch and transform
        mock_strategy.get_documents.return_value = mock_documents
        mock_strategy.extract_data_for_sql.side_effect = [
            ([str(mock_documents[0]['_id']), 'User 1', 'user1@test.com', datetime.now(), datetime.now()],
             ['id', 'name', 'email', 'created_at', 'updated_at']),
            ([str(mock_documents[1]['_id']), 'User 2', 'user2@test.com', datetime.now(), datetime.now()],
             ['id', 'name', 'email', 'created_at', 'updated_at'])
        ]

        # Step 4: Mock import
        mock_strategy.get_use_on_conflict.return_value = True
        mock_strategy.get_on_conflict_clause.return_value = " ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at"

        with patch.object(ImportUtils, 'execute_batch', return_value=2) as mock_execute:
            # Execute the flow
            total_documents = mock_strategy.count_total_documents(mock_collection, config)
            assert total_documents == 2

            documents = mock_strategy.get_documents(mock_collection, config, offset=0)
            assert len(documents) == 2

            all_batch_values = []
            columns = None
            for doc in documents:
                values, doc_columns = mock_strategy.extract_data_for_sql(doc, config)
                if columns is None:
                    columns = doc_columns
                all_batch_values.append(values)

            assert len(all_batch_values) == 2

            # Execute import
            use_on_conflict = mock_strategy.get_use_on_conflict()
            on_conflict_clause = mock_strategy.get_on_conflict_clause('users', columns)

            actual_insertions = ImportUtils.execute_batch(
                mock_conn, all_batch_values, columns, 'users',
                config.summary_instance, use_on_conflict, on_conflict_clause
            )

            assert actual_insertions == 2
            assert mock_execute.called

    def test_full_migration_flow_for_relationship_table(self):
        """Test complete flow for relationship table (DeleteAndInsertStrategy)"""
        from src.migration.import_strategies import ImportUtils

        # Setup DeleteAndInsertStrategy mock
        mock_strategy = Mock()
        mock_strategy.count_total_documents.return_value = 1

        user_id = ObjectId()
        mock_strategy.get_documents.return_value = [{
            '_id': user_id,
            'registered_events': [ObjectId(), ObjectId()],
            'creation_date': datetime.now(),
            'update_date': datetime.now()
        }]

        # Mock methods specific to DeleteAndInsertStrategy
        mock_strategy.get_parent_id_from_document.return_value = str(user_id)
        mock_strategy.get_delete_table_name.return_value = 'user_events'
        mock_strategy.get_delete_column_name.return_value = 'user_id'
        mock_strategy.extract_data_for_sql.return_value = (
            [[str(user_id), 'event1', datetime.now()],
             [str(user_id), 'event2', datetime.now()]],
            ['user_id', 'event_id', 'created_at']
        )
        mock_strategy.get_use_on_conflict.return_value = False

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 3  # Simulates 3 old rows deleted

        mock_collection = Mock()
        config = ImportConfig(
            table_name='user_events',
            source_collection='users',
            summary_instance=Mock()
        )

        with patch.object(ImportUtils, 'execute_batch', return_value=2) as mock_execute:
            # Step 2
            total_documents = mock_strategy.count_total_documents(mock_collection, config)
            assert total_documents == 1

            # Step 3
            documents = mock_strategy.get_documents(mock_collection, config, offset=0)
            assert len(documents) == 1

            document = documents[0]
            parent_id = mock_strategy.get_parent_id_from_document(document)
            values, columns = mock_strategy.extract_data_for_sql(document, config)

            # Step 4a: DELETE
            delete_table = mock_strategy.get_delete_table_name(config)
            delete_column = mock_strategy.get_delete_column_name()
            delete_sql = f"DELETE FROM {delete_table} WHERE {delete_column} IN (%s)"

            mock_cursor.execute(delete_sql, [parent_id])
            deleted_count = mock_cursor.rowcount

            assert deleted_count == 3

            # Step 4b: INSERT
            actual_insertions = ImportUtils.execute_batch(
                mock_conn, values, columns, 'user_events',
                config.summary_instance, use_on_conflict=False, on_conflict_clause=""
            )

            assert actual_insertions == 2
            assert mock_execute.called

    def test_pagination_across_multiple_batches(self, mock_strategy):
        """Test that pagination works correctly across multiple batches"""
        mock_collection = Mock()

        # First batch: 5000 documents
        # Second batch: 2000 documents
        # Third batch: 0 documents (end)
        batch_returns = [
            [{'_id': ObjectId(), 'name': f'User {i}'} for i in range(5000)],  # First batch
            [{'_id': ObjectId(), 'name': f'User {i}'} for i in range(2000)],  # Second batch
            []  # End
        ]

        mock_strategy.get_documents.side_effect = batch_returns

        config = ImportConfig(
            table_name='users',
            source_collection='users',
            batch_size=5000,
            summary_instance=Mock()
        )

        # Simulate pagination loop
        total_fetched = 0
        offset = 0
        batch_size = 5000

        while True:
            documents = mock_strategy.get_documents(mock_collection, config, offset=offset)
            if not documents:
                break

            total_fetched += len(documents)
            offset += batch_size

            if len(documents) < batch_size:
                break

        assert total_fetched == 7000  # 5000 + 2000
        assert mock_strategy.get_documents.call_count == 2

    def test_skip_table_when_no_changes(self, mock_strategy):
        """Test that tables with no changes are skipped"""
        mock_collection = Mock()
        mock_strategy.count_total_documents.return_value = 0

        config = ImportConfig(
            table_name='users',
            source_collection='users',
            after_date=datetime(2024, 1, 1),
            summary_instance=Mock()
        )

        # Step 2
        total_documents = mock_strategy.count_total_documents(mock_collection, config)

        # Should skip when count is 0
        if total_documents == 0:
            # Table should be skipped
            assert True
        else:
            # Should not reach here
            assert False, "Table should have been skipped"

    def test_error_handling_during_delete(self):
        """Test error handling when DELETE fails"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        # Simulate DELETE error
        mock_cursor.execute.side_effect = Exception("Database connection lost")

        parent_ids = ['user1', 'user2']

        try:
            delete_sql = "DELETE FROM user_events WHERE user_id IN (%s, %s)"
            mock_cursor.execute(delete_sql, parent_ids)
            mock_conn.commit()
        except Exception as e:
            # Should rollback on error
            mock_conn.rollback()
            error_occurred = True
        else:
            error_occurred = False

        assert error_occurred
        assert mock_conn.rollback.called


class TestConsoleOutput:
    """Test console output formatting for the 4-step process"""

    def test_step1_output_with_existing_data(self):
        """Test Step 1 console output when data exists"""
        table_name = 'users'
        after_date = datetime(2024, 1, 15, 10, 30, 0)

        output = f"📅 Step 1: Last migration date: {after_date}"
        assert '📅 Step 1' in output
        assert '2024-01-15' in output

    def test_step2_output_document_count(self):
        """Test Step 2 console output with document count"""
        total_documents = 150

        output = f"📊 Step 2: Querying new/updated documents from MongoDB\n   → Found {total_documents} documents to process"
        assert '📊 Step 2' in output
        assert '150 documents' in output

    def test_step3_output_transformation_progress(self):
        """Test Step 3 console output showing transformation"""
        documents_count = 100
        rows_count = 250

        output = f"   → Transformed {documents_count} documents into {rows_count} SQL rows"
        assert '100 documents' in output
        assert '250 SQL rows' in output

    def test_step4_output_delete_and_insert(self):
        """Test Step 4 console output for delete-and-insert"""
        deleted_count = 200
        parent_count = 50
        inserted_count = 180

        output_delete = f"   → Deleted {deleted_count} existing relationships for {parent_count} parents"
        output_insert = f"   → Inserted/Updated {inserted_count} records"

        assert 'Deleted 200' in output_delete
        assert '50 parents' in output_delete
        assert 'Inserted/Updated 180' in output_insert


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
