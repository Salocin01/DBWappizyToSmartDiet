"""
Tests for UsersTargetsStrategy

Tests the refactored UsersTargetsStrategy that extends DeleteAndInsertStrategy
Tests multi-array consolidation with type discrimination
"""

import pytest
from unittest.mock import Mock, patch
from bson import ObjectId
from datetime import datetime

from src.schemas.schemas import _create_users_targets_strategy
from src.migration.import_strategies import ImportConfig, ImportUtils


class TestUsersTargetsStrategy:
    """Test suite for UsersTargetsStrategy"""

    @pytest.fixture
    def strategy(self):
        """Create a UsersTargetsStrategy instance"""
        return _create_users_targets_strategy()

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
            table_name='users_targets',
            source_collection='users',
            batch_size=5000,
            after_date=datetime(2024, 1, 1),
            summary_instance=Mock()
        )

    def test_count_total_documents_with_or_filter(self, strategy, mock_collection, import_config):
        """Test counting documents with $or filter for multiple target arrays"""
        strategy.count_total_documents(mock_collection, import_config)

        # Verify MongoDB query includes $or condition for all three arrays
        call_args = mock_collection.count_documents.call_args[0][0]
        assert '$or' in call_args

        # The $or should have two levels: one for arrays, one for dates
        # Check that target array filters are present
        query_str = str(call_args)
        assert 'targets' in query_str
        assert 'specificity_targets' in query_str
        assert 'health_targets' in query_str

    def test_get_documents_with_multi_array_filter(self, strategy, mock_collection, import_config):
        """Test fetching documents with correct multi-array filter"""
        mock_cursor = Mock()
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = []
        mock_collection.find.return_value = mock_cursor

        strategy.get_documents(mock_collection, import_config, offset=0)

        # Verify filter includes all three arrays
        call_args = mock_collection.find.call_args[0]
        mongo_filter = call_args[0]

        assert '$or' in mongo_filter

    def test_get_documents_projection(self, strategy, mock_collection, import_config):
        """Test that document projection includes all target fields"""
        mock_cursor = Mock()
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = []
        mock_collection.find.return_value = mock_cursor

        strategy.get_documents(mock_collection, import_config, offset=0)

        # Verify projection includes all necessary fields
        call_args = mock_collection.find.call_args[0]
        projection = call_args[1] if len(call_args) > 1 else {}

        assert '_id' in projection
        assert 'targets' in projection
        assert 'specificity_targets' in projection
        assert 'health_targets' in projection
        assert 'creation_date' in projection
        assert 'update_date' in projection

    def test_extract_data_basic_targets_only(self, strategy, import_config):
        """Test extracting data when only basic targets are present"""
        user_id = ObjectId()
        target_id_1 = ObjectId()
        target_id_2 = ObjectId()

        document = {
            '_id': user_id,
            'targets': [target_id_1, target_id_2],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify columns
        assert columns == ['user_id', 'target_id', 'type', 'created_at', 'updated_at']

        # Verify we got 2 rows with type='basic'
        assert len(values) == 2

        assert values[0][0] == str(user_id)  # user_id
        assert values[0][1] == str(target_id_1)  # target_id
        assert values[0][2] == 'basic'  # type
        assert values[0][3] == datetime(2024, 1, 15)  # created_at
        assert values[0][4] == datetime(2024, 1, 20)  # updated_at

        assert values[1][0] == str(user_id)
        assert values[1][1] == str(target_id_2)
        assert values[1][2] == 'basic'

    def test_extract_data_specificity_targets_only(self, strategy, import_config):
        """Test extracting data when only specificity targets are present"""
        user_id = ObjectId()
        target_id = ObjectId()

        document = {
            '_id': user_id,
            'specificity_targets': [target_id],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify we got 1 row with type='specificity'
        assert len(values) == 1
        assert values[0][2] == 'specificity'

    def test_extract_data_health_targets_only(self, strategy, import_config):
        """Test extracting data when only health targets are present"""
        user_id = ObjectId()
        target_id = ObjectId()

        document = {
            '_id': user_id,
            'health_targets': [target_id],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify we got 1 row with type='health'
        assert len(values) == 1
        assert values[0][2] == 'health'

    def test_extract_data_all_three_arrays(self, strategy, import_config):
        """Test extracting data when all three target arrays are present"""
        user_id = ObjectId()
        basic_target_1 = ObjectId()
        basic_target_2 = ObjectId()
        specificity_target = ObjectId()
        health_target_1 = ObjectId()
        health_target_2 = ObjectId()

        document = {
            '_id': user_id,
            'targets': [basic_target_1, basic_target_2],
            'specificity_targets': [specificity_target],
            'health_targets': [health_target_1, health_target_2],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify we got 5 rows total (2 basic + 1 specificity + 2 health)
        assert len(values) == 5

        # Count by type
        types = [row[2] for row in values]
        assert types.count('basic') == 2
        assert types.count('specificity') == 1
        assert types.count('health') == 2

        # Verify all rows have the same user_id
        user_ids = [row[0] for row in values]
        assert all(uid == str(user_id) for uid in user_ids)

        # Verify all target_ids are unique and correctly assigned
        basic_rows = [row for row in values if row[2] == 'basic']
        assert str(basic_target_1) in [row[1] for row in basic_rows]
        assert str(basic_target_2) in [row[1] for row in basic_rows]

        specificity_rows = [row for row in values if row[2] == 'specificity']
        assert str(specificity_target) == specificity_rows[0][1]

        health_rows = [row for row in values if row[2] == 'health']
        assert str(health_target_1) in [row[1] for row in health_rows]
        assert str(health_target_2) in [row[1] for row in health_rows]

    def test_extract_data_empty_arrays(self, strategy, import_config):
        """Test extracting data when all arrays are empty"""
        user_id = ObjectId()

        document = {
            '_id': user_id,
            'targets': [],
            'specificity_targets': [],
            'health_targets': [],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should return empty batch
        assert len(values) == 0

    def test_extract_data_missing_arrays(self, strategy, import_config):
        """Test extracting data when target fields are missing from document"""
        user_id = ObjectId()
        basic_target = ObjectId()

        document = {
            '_id': user_id,
            'targets': [basic_target],
            # specificity_targets and health_targets are missing
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should only extract basic target
        assert len(values) == 1
        assert values[0][2] == 'basic'

    def test_get_parent_id_from_document(self, strategy):
        """Test extracting user_id from document"""
        user_id = ObjectId()
        document = {'_id': user_id, 'name': 'Test User'}

        parent_id = strategy.get_parent_id_from_document(document)

        assert parent_id == str(user_id)

    def test_get_delete_table_name(self, strategy, import_config):
        """Test that correct table name is returned for deletion"""
        table_name = strategy.get_delete_table_name(import_config)

        assert table_name == 'users_targets'

    def test_get_delete_column_name(self, strategy):
        """Test that correct column name is returned for WHERE clause"""
        column_name = strategy.get_delete_column_name()

        assert column_name == 'user_id'

    def test_progress_message_format(self, strategy):
        """Test custom progress message formatting"""
        message = strategy.get_progress_message(
            processed=50,
            total=100,
            table_name='users_targets',
            total_records=200
        )

        # Should include user count and relationship count
        assert '50' in message
        assert '100' in message
        assert '200' in message
        assert 'users' in message.lower()
        assert 'user-target' in message.lower()

    def test_inherits_from_delete_and_insert_strategy(self, strategy):
        """Test that UsersTargetsStrategy correctly extends DeleteAndInsertStrategy"""
        from src.migration.import_strategies import DeleteAndInsertStrategy

        assert isinstance(strategy, DeleteAndInsertStrategy)

    def test_does_not_use_on_conflict(self, strategy):
        """Test that strategy uses delete-and-insert instead of ON CONFLICT"""
        # DeleteAndInsertStrategy should return False
        assert strategy.get_use_on_conflict() is False

    def test_handles_missing_dates(self, strategy, import_config):
        """Test handling documents with missing creation/update dates"""
        user_id = ObjectId()
        target_id = ObjectId()

        document = {
            '_id': user_id,
            'targets': [target_id],
            # No creation_date or update_date
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should still create row, but with None dates
        assert len(values) == 1
        assert values[0][0] == str(user_id)
        assert values[0][1] == str(target_id)
        assert values[0][2] == 'basic'
        # created_at and updated_at will be None

    def test_type_discrimination_accuracy(self, strategy, import_config):
        """Test that type discrimination is accurate for all three arrays"""
        user_id = ObjectId()
        target1 = ObjectId()
        target2 = ObjectId()
        target3 = ObjectId()

        document = {
            '_id': user_id,
            'targets': [target1],
            'specificity_targets': [target2],
            'health_targets': [target3],
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Verify each target has the correct type
        target_type_map = {row[1]: row[2] for row in values}

        assert target_type_map[str(target1)] == 'basic'
        assert target_type_map[str(target2)] == 'specificity'
        assert target_type_map[str(target3)] == 'health'

    def test_full_export_cycle(self, strategy, import_config):
        """Integration test: full export cycle with mocked database"""
        user1_id = ObjectId()
        user2_id = ObjectId()
        target1 = ObjectId()
        target2 = ObjectId()
        target3 = ObjectId()

        mock_collection = Mock()
        mock_collection.count_documents.return_value = 2

        # Mock find to return test documents
        mock_cursor = Mock()
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = [
            {
                '_id': user1_id,
                'targets': [target1, target2],
                'specificity_targets': [target3],
                'creation_date': datetime(2024, 1, 15),
                'update_date': datetime(2024, 1, 20)
            },
            {
                '_id': user2_id,
                'health_targets': [target1],
                'creation_date': datetime(2024, 1, 10),
                'update_date': datetime(2024, 1, 18)
            }
        ]
        mock_collection.find.return_value = mock_cursor

        mock_conn = Mock()
        mock_conn.cursor.return_value = Mock()

        with patch.object(ImportUtils, 'execute_batch', return_value=4) as mock_execute:
            result = strategy.export_data(mock_conn, mock_collection, import_config)

            # Verify execute_batch was called
            assert mock_execute.called

            # Verify correct number of relationships inserted
            # user1: 2 basic + 1 specificity = 3
            # user2: 1 health = 1
            # Total = 4
            batch_values = mock_execute.call_args[0][1]
            assert len(batch_values) == 4

            # Verify user1 has correct targets
            user1_targets = [v for v in batch_values if v[0] == str(user1_id)]
            assert len(user1_targets) == 3
            user1_types = [v[2] for v in user1_targets]
            assert user1_types.count('basic') == 2
            assert user1_types.count('specificity') == 1

            # Verify user2 has correct targets
            user2_targets = [v for v in batch_values if v[0] == str(user2_id)]
            assert len(user2_targets) == 1
            assert user2_targets[0][2] == 'health'

    def test_multi_array_consolidation_consistency(self, strategy, import_config):
        """Test that consolidation of multiple arrays maintains data consistency"""
        user_id = ObjectId()

        # Create overlapping target IDs to ensure they're kept separate by type
        shared_target = ObjectId()

        document = {
            '_id': user_id,
            'targets': [shared_target],
            'specificity_targets': [shared_target],  # Same ID, different type
            'creation_date': datetime(2024, 1, 15),
            'update_date': datetime(2024, 1, 20)
        }

        values, columns = strategy.extract_data_for_sql(document, import_config)

        # Should have 2 rows with same target_id but different types
        assert len(values) == 2
        assert values[0][1] == str(shared_target)
        assert values[1][1] == str(shared_target)
        assert values[0][2] != values[1][2]  # Types should be different

        types = {row[2] for row in values}
        assert 'basic' in types
        assert 'specificity' in types


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
