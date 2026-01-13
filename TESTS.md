# Test Documentation

This document describes the test suite for the DBWappizyToSmartDiet database migration tool.

## Overview

The test suite comprehensively covers the 4-step migration pattern and all three migration strategy types:
1. **DirectTranslationStrategy** - Simple 1:1 table mapping
2. **ArrayExtractionStrategy** - Array field extraction
3. **DeleteAndInsertStrategy** - Relationship table synchronization

## Test Structure

```
tests/
├── test_transfert_data.py              # Migration flow via runner/strategies
├── test_delete_and_insert_strategy.py  # DeleteAndInsertStrategy base class
├── test_user_events_strategy.py        # UserEventsStrategy implementation
├── test_users_targets_strategy.py      # UsersTargetsStrategy implementation
└── test_migration_integration.py       # End-to-end integration tests
```

## Test Files Description

### 1. `test_transfert_data.py`

**Purpose:** Tests the migration flow executed by `src/migration/runner.py` via `transfert_data.py`.

**What it tests:**
- ✅ **Step 1**: Getting last migration date from PostgreSQL
- ✅ **Step 2**: Querying new/updated documents from MongoDB
- ✅ **Step 3**: Transforming documents to SQL format (with pagination)
- ✅ **Step 4**: Executing import (with DELETE for relationships)

**Key test scenarios:**
```python
test_step1_get_last_migration_date()          # PostgreSQL date query
test_step2_count_documents()                  # MongoDB document counting
test_step3_transform_single_document()        # Single document transformation
test_step3_transform_batch_of_documents()     # Batch transformation
test_step3_handles_multiple_rows_per_document() # Array extraction (1 doc → N rows)
test_step4_upsert_with_on_conflict()          # ON CONFLICT DO UPDATE
test_step4_delete_and_insert_pattern()        # DELETE + INSERT pattern (strategy-driven)
test_full_migration_flow_for_simple_table()   # End-to-end for users table
test_full_migration_flow_for_relationship_table() # End-to-end for user_events
test_pagination_across_multiple_batches()     # Handling 5000+ documents
test_skip_table_when_no_changes()            # Optimization when count=0
test_error_handling_during_delete()          # Rollback on errors
```

**Lines of code:** ~493 lines
**Test count:** 15 comprehensive tests

### 2. `test_delete_and_insert_strategy.py`

**Purpose:** Tests the DeleteAndInsertStrategy base class and template method pattern.

**What it tests:**
- ✅ Template method flow (correct order of operations)
- ✅ DELETE query construction and execution
- ✅ INSERT query execution
- ✅ Batch processing and pagination
- ✅ Error handling during DELETE
- ✅ Multiple batch processing

**Key test scenarios:**
```python
test_template_method_flow()                   # Verifies all methods called in order
test_delete_existing_relationships()          # DELETE SQL generation and execution
test_insert_fresh_relationships()             # INSERT with correct data
test_batch_processing_pagination()            # Pagination across batches
test_error_handling_during_delete()          # Exception handling and rollback
test_no_on_conflict_for_delete_and_insert()  # Verifies no ON CONFLICT used
test_handles_multiple_batches()              # Multi-batch scenarios
```

**Lines of code:** ~350 lines
**Test count:** 12 tests

### 3. `test_user_events_strategy.py`

**Purpose:** Tests UserEventsStrategy (user → events relationship).

**What it tests:**
- ✅ MongoDB filter construction (registered_events existence check)
- ✅ Document fetching with correct projection
- ✅ Data extraction for ObjectId arrays
- ✅ Data extraction for embedded documents
- ✅ Data extraction for mixed formats
- ✅ Parent ID extraction
- ✅ Delete configuration

**Key test scenarios:**
```python
test_count_total_documents_with_filter()      # MongoDB $or filter with arrays
test_extract_data_for_objectid_array()        # Array of ObjectIds
test_extract_data_for_embedded_documents()    # Array of embedded docs
test_extract_data_for_mixed_formats()         # Mixed ObjectIds and docs
test_get_parent_id_from_document()           # user_id extraction
test_get_delete_table_name()                 # Returns 'user_events'
test_get_delete_column_name()                # Returns 'user_id'
test_full_export_cycle()                     # Complete migration cycle
```

**Lines of code:** ~320 lines
**Test count:** 14 tests

### 4. `test_users_targets_strategy.py`

**Purpose:** Tests UsersTargetsStrategy (multi-array consolidation with type discrimination).

**What it tests:**
- ✅ Multi-array MongoDB filter ($or with 3 arrays)
- ✅ Type discrimination (basic, specificity, health)
- ✅ Array consolidation into single table
- ✅ Correct field projection
- ✅ Parent ID extraction
- ✅ Delete configuration

**Key test scenarios:**
```python
test_count_total_documents_with_or_filter()   # $or filter for 3 arrays
test_extract_data_basic_targets_only()        # Only targets[] array
test_extract_data_specificity_targets_only()  # Only specificity_targets[]
test_extract_data_health_targets_only()       # Only health_targets[]
test_extract_data_all_three_arrays()          # All 3 arrays present
test_type_discrimination_accuracy()           # Correct type assignment
test_multi_array_consolidation_consistency()  # Same target_id, different types
test_full_export_cycle()                     # Complete migration cycle
```

**Lines of code:** ~380 lines
**Test count:** 16 tests

### 5. `test_migration_integration.py`

**Purpose:** End-to-end integration tests for realistic migration scenarios.

**What it tests:**
- ✅ Incremental migration behavior
- ✅ Delete-and-insert correctness (handles removals)
- ✅ Data consistency across migrations
- ✅ Batch processing performance
- ✅ Error recovery

**Key test scenarios:**
```python
test_get_last_insert_date_with_data()         # Incremental sync setup
test_user_events_handles_event_removal()      # Removing items from MongoDB arrays
test_users_targets_handles_array_changes()    # Multi-array modifications
test_incremental_sync_does_not_duplicate_data() # ON CONFLICT prevents duplicates
test_delete_and_insert_maintains_referential_integrity() # FK integrity
test_handles_large_batches()                  # 100+ documents
test_handles_delete_error_gracefully()        # Error recovery
test_uses_pagination_for_large_datasets()     # 5000+ documents
```

**Lines of code:** ~330 lines
**Test count:** 10 tests

## Running Tests

### Prerequisites

Install pytest:
```bash
pip install pytest
```

### Run All Tests

```bash
# Run all tests with verbose output
python3 -m pytest tests/ -v

# Run all tests with summary
python3 -m pytest tests/
```

### Run Specific Test Files

```bash
# Test the main migration flow (explicit 4-step pattern)
python3 -m pytest tests/test_transfert_data.py -v

# Test DeleteAndInsertStrategy base class
python3 -m pytest tests/test_delete_and_insert_strategy.py -v

# Test UserEventsStrategy
python3 -m pytest tests/test_user_events_strategy.py -v

# Test UsersTargetsStrategy
python3 -m pytest tests/test_users_targets_strategy.py -v

# Test integration scenarios
python3 -m pytest tests/test_migration_integration.py -v
```

### Run Specific Test Methods

```bash
# Run a single test method
python3 -m pytest tests/test_transfert_data.py::TestExplicitFourStepFlow::test_step1_get_last_migration_date -v

# Run all tests in a class
python3 -m pytest tests/test_transfert_data.py::TestExplicitFourStepFlow -v
```

### Run Tests with Coverage

```bash
# Install coverage tool
pip install pytest-cov

# Run tests with coverage report
python3 -m pytest tests/ --cov=src --cov=transfert_data --cov-report=html

# View coverage report
open htmlcov/index.html
```

### Run Tests with Output

```bash
# Show print statements during tests
python3 -m pytest tests/ -v -s

# Show only failed tests with full output
python3 -m pytest tests/ -v --tb=short
```

## Test Output Example

```
tests/test_transfert_data.py::TestExplicitFourStepFlow::test_step1_get_last_migration_date PASSED
tests/test_transfert_data.py::TestExplicitFourStepFlow::test_step2_count_documents PASSED
tests/test_transfert_data.py::TestExplicitFourStepFlow::test_step3_transform_single_document PASSED
tests/test_transfert_data.py::TestExplicitFourStepFlow::test_step4_upsert_with_on_conflict PASSED
tests/test_transfert_data.py::TestExplicitFourStepFlow::test_full_migration_flow_for_simple_table PASSED
tests/test_transfert_data.py::TestExplicitFourStepFlow::test_full_migration_flow_for_relationship_table PASSED
...

==================== 67 passed in 2.34s ====================
```

## Test Coverage Summary

| Component | Coverage | Test File |
|-----------|----------|-----------|
| **4-Step Flow** | ✅ Complete | test_transfert_data.py |
| **DeleteAndInsertStrategy** | ✅ Complete | test_delete_and_insert_strategy.py |
| **UserEventsStrategy** | ✅ Complete | test_user_events_strategy.py |
| **UsersTargetsStrategy** | ✅ Complete | test_users_targets_strategy.py |
| **Integration Scenarios** | ✅ Complete | test_migration_integration.py |
| **DirectTranslationStrategy** | ✅ Covered | test_transfert_data.py, test_migration_integration.py |
| **ArrayExtractionStrategy** | ⚠️ Partial | test_migration_integration.py |

**Total Test Count:** ~67 tests
**Total Test Code:** ~1,873 lines

## Adding New Tests

### For New Strategy Types

1. Create a new test file: `tests/test_your_strategy.py`
2. Test these methods:
   - `count_total_documents()`
   - `get_documents()`
   - `extract_data_for_sql()`
   - Strategy-specific methods

Example:
```python
from src.schemas.schemas import _create_your_strategy
from src.migration.import_strategies import ImportConfig

class TestYourStrategy:
    @pytest.fixture
    def strategy(self):
        return _create_your_strategy()

    def test_count_total_documents(self, strategy):
        # Test counting logic
        pass

    def test_extract_data_for_sql(self, strategy):
        # Test data transformation
        pass
```

### For New Migration Scenarios

Add to `test_migration_integration.py`:
```python
def test_your_scenario():
    """Test description"""
    # Setup
    # Execute migration steps
    # Assert expectations
    pass
```

### For Main Flow Modifications

Add to `test_transfert_data.py` (runner-driven flow):
```python
def test_your_new_feature():
    """Test description"""
    # Test the explicit 4-step flow with your feature
    pass
```

## Testing Best Practices

### 1. Use Mocks for External Dependencies
```python
from unittest.mock import Mock, patch

mock_conn = Mock()  # Mock PostgreSQL connection
mock_collection = Mock()  # Mock MongoDB collection
```

### 2. Test One Thing Per Test
```python
# Good
def test_step1_get_last_migration_date():
    # Tests only Step 1
    pass

# Bad
def test_everything():
    # Tests steps 1, 2, 3, and 4 together
    pass
```

### 3. Use Descriptive Test Names
```python
# Good
def test_delete_and_insert_pattern_handles_removal():
    pass

# Bad
def test_delete():
    pass
```

### 4. Use Fixtures for Common Setup
```python
@pytest.fixture
def mock_documents(self):
    return [{'_id': ObjectId(), 'name': 'Test'}]
```

### 5. Assert Specific Behaviors
```python
# Good
assert deleted_count == 3
assert 'DELETE FROM user_events' in delete_sql

# Bad
assert result  # Too vague
```

## Continuous Integration

### GitHub Actions Example

Create `.github/workflows/tests.yml`:
```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v2

    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.12'

    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest pytest-cov

    - name: Run tests
      run: pytest tests/ -v --cov=src --cov=transfert_data

    - name: Upload coverage
      uses: codecov/codecov-action@v2
```

## Troubleshooting

### ImportError: No module named 'pytest'
```bash
pip install pytest
```

### Tests fail with "ModuleNotFoundError"
Ensure you run tests from the project root:
```bash
cd /path/to/DBWappizyToSmartDiet
python3 -m pytest tests/
```

### Mock connection issues
Ensure mocks are set up correctly:
```python
mock_conn = Mock()
mock_cursor = Mock()
mock_conn.cursor.return_value = mock_cursor
```

### Tests are slow
Use mocks instead of real database connections. All tests should complete in < 5 seconds.

## Future Test Enhancements

- [ ] Add performance benchmarks
- [ ] Add mutation testing
- [ ] Add property-based testing (hypothesis)
- [ ] Add database fixture setup/teardown for integration tests
- [ ] Add test for SQL file generation (DIRECT_IMPORT=False)
- [ ] Add test coverage for ArrayExtractionStrategy

## References

- [pytest Documentation](https://docs.pytest.org/)
- [unittest.mock Documentation](https://docs.python.org/3/library/unittest.mock.html)
- Project architecture: See `CLAUDE.md`
- Migration strategies: See `src/migration/import_strategies.py`
