"""
Schema comparison and validation module for PostgreSQL.

This module provides functions to:
- Introspect current PostgreSQL table structures
- Compare YAML-defined schemas with actual database schemas
- Generate ALTER TABLE statements to sync schemas
- Validate safety of schema changes (e.g., NOT NULL constraints)
"""

from typing import Dict, List, Any, Optional
from src.schemas.table_schemas import TableSchema, ColumnDefinition


def get_current_table_columns(conn, table_name: str) -> Dict[str, Dict[str, Any]]:
    """
    Query PostgreSQL information_schema to get current table column definitions.

    Args:
        conn: PostgreSQL connection object
        table_name: Name of the table to introspect

    Returns:
        Dict mapping column names to their properties:
        {
            'column_name': {
                'data_type': 'character varying',
                'character_maximum_length': 255,
                'is_nullable': 'NO',
                'column_default': None
            }
        }
    """
    cursor = conn.cursor()

    query = """
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """

    cursor.execute(query, (table_name,))
    rows = cursor.fetchall()

    columns = {}
    for row in rows:
        column_name, data_type, max_length, is_nullable, column_default = row
        columns[column_name] = {
            'data_type': data_type,
            'character_maximum_length': max_length,
            'is_nullable': is_nullable,
            'column_default': column_default
        }

    return columns


def get_current_foreign_keys(conn, table_name: str) -> List[Dict[str, str]]:
    """
    Query PostgreSQL information_schema to get current foreign key constraints.

    Args:
        conn: PostgreSQL connection object
        table_name: Name of the table to introspect

    Returns:
        List of foreign key definitions:
        [
            {
                'column_name': 'user_id',
                'foreign_table': 'users',
                'foreign_column': 'id'
            }
        ]
    """
    cursor = conn.cursor()

    query = """
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table,
            ccu.column_name AS foreign_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = %s
    """

    cursor.execute(query, (table_name,))
    rows = cursor.fetchall()

    foreign_keys = []
    for row in rows:
        column_name, foreign_table, foreign_column = row
        foreign_keys.append({
            'column_name': column_name,
            'foreign_table': foreign_table,
            'foreign_column': foreign_column
        })

    return foreign_keys


def normalize_sql_type(sql_type: str) -> str:
    """
    Normalize SQL type string for comparison.

    Handles variations like:
    - VARCHAR(255) -> character varying
    - INTEGER -> integer
    - TIMESTAMP -> timestamp without time zone
    """
    sql_type_upper = sql_type.upper().strip()

    # Map common type aliases to PostgreSQL internal types
    type_mapping = {
        'VARCHAR': 'character varying',
        'TEXT': 'text',
        'INTEGER': 'integer',
        'INT': 'integer',
        'SMALLINT': 'smallint',
        'BIGINT': 'bigint',
        'BOOLEAN': 'boolean',
        'BOOL': 'boolean',
        'TIMESTAMP': 'timestamp without time zone',
        'DATE': 'date',
        'SERIAL': 'integer',  # SERIAL is stored as integer
        'BIGSERIAL': 'bigint'
    }

    # Extract base type (remove parentheses and content)
    base_type = sql_type_upper.split('(')[0].strip()

    return type_mapping.get(base_type, base_type.lower())


def validate_not_null_safety(conn, table_name: str, column_name: str) -> tuple[bool, int]:
    """
    Check if adding a NOT NULL constraint would be safe.

    A NOT NULL constraint is safe to add if there are no existing NULL values
    in the column.

    Args:
        conn: PostgreSQL connection object
        table_name: Name of the table
        column_name: Name of the column to check

    Returns:
        Tuple of (is_safe: bool, null_count: int)
        - is_safe: True if no NULL values exist, False otherwise
        - null_count: Number of rows with NULL values
    """
    cursor = conn.cursor()

    # Note: This assumes the column already exists (for checking before altering)
    # For new columns, this check is not applicable
    query = f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE {column_name} IS NULL
    """

    try:
        cursor.execute(query)
        null_count = cursor.fetchone()[0]
        return (null_count == 0, null_count)
    except Exception as e:
        # Column doesn't exist yet, which is fine for new columns
        return (True, 0)


def compare_table_schema(yaml_schema: TableSchema, conn, table_name: str) -> Dict[str, Any]:
    """
    Compare YAML-defined schema with actual PostgreSQL table structure.

    Currently only detects added columns (safest operation).
    Does not detect:
    - Modified columns (type changes, constraint changes)
    - Removed columns
    - Modified constraints

    Args:
        yaml_schema: The TableSchema object from YAML definition
        conn: PostgreSQL connection object
        table_name: Name of the table to compare

    Returns:
        Dictionary with comparison results:
        {
            'added_columns': [ColumnDefinition, ...],
            'missing_foreign_keys': [ColumnDefinition, ...],
            'errors': [{'column': str, 'message': str}, ...],
            'status': 'ok' | 'needs_update' | 'error'
        }
    """
    # Get current PostgreSQL schema
    current_columns = get_current_table_columns(conn, table_name)
    current_fks = get_current_foreign_keys(conn, table_name)

    # Build set of current column names and FK columns
    current_column_names = set(current_columns.keys())
    current_fk_columns = {fk['column_name'] for fk in current_fks}

    # Compare with YAML schema
    added_columns = []
    missing_foreign_keys = []
    errors = []

    for col_def in yaml_schema.columns:
        if col_def.name not in current_column_names:
            # This column exists in YAML but not in PostgreSQL
            added_columns.append(col_def)

            # If it has a foreign key, we'll need to add that too
            if col_def.foreign_key:
                missing_foreign_keys.append(col_def)

    # Determine status
    if errors:
        status = 'error'
    elif added_columns:
        status = 'needs_update'
    else:
        status = 'ok'

    return {
        'added_columns': added_columns,
        'missing_foreign_keys': missing_foreign_keys,
        'errors': errors,
        'status': status
    }


def generate_alter_statements(
    table_name: str,
    differences: Dict[str, Any],
    conn
) -> tuple[List[str], List[Dict[str, str]]]:
    """
    Generate ALTER TABLE statements to sync schema.

    Only generates ADD COLUMN statements (safest operation).

    Args:
        table_name: Name of the table to alter
        differences: Output from compare_table_schema()
        conn: PostgreSQL connection for validation queries

    Returns:
        Tuple of (statements: List[str], errors: List[Dict])
        - statements: List of SQL ALTER TABLE statements
        - errors: List of error messages for unsafe operations
    """
    statements = []
    errors = []

    added_columns = differences.get('added_columns', [])

    for col_def in added_columns:
        # Build column definition
        col_type = col_def.sql_type

        # Check if we can safely add NOT NULL
        can_add_not_null = True
        if not col_def.nullable:
            # For new columns, NOT NULL is safe only if:
            # 1. Table is empty, OR
            # 2. Column has a DEFAULT value

            # Check if table has data
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]

            if row_count > 0:
                # Table has data - NOT NULL requires a DEFAULT
                # For now, we'll add the column as nullable and report an error
                can_add_not_null = False
                errors.append({
                    'column': col_def.name,
                    'message': f"Cannot add NOT NULL constraint to {table_name}.{col_def.name}",
                    'reason': f"{row_count} existing rows would have NULL values",
                    'solution': "Either: (1) Make column nullable in YAML, or (2) Add DEFAULT value, or (3) Populate data first"
                })

        # Build ALTER TABLE ADD COLUMN statement
        if can_add_not_null and not col_def.nullable:
            alter_stmt = f"ALTER TABLE {table_name} ADD COLUMN {col_def.name} {col_type} NOT NULL"
        else:
            alter_stmt = f"ALTER TABLE {table_name} ADD COLUMN {col_def.name} {col_type}"

        statements.append(alter_stmt)

    # Add foreign key constraints (separate statements, after columns are added)
    for col_def in differences.get('missing_foreign_keys', []):
        if col_def.foreign_key:
            # Parse foreign key: "table(column)"
            ref_table, ref_column = col_def.foreign_key.replace(')', '').split('(')
            fk_name = f"fk_{table_name}_{col_def.name}"

            fk_stmt = (
                f"ALTER TABLE {table_name} "
                f"ADD CONSTRAINT {fk_name} "
                f"FOREIGN KEY ({col_def.name}) "
                f"REFERENCES {ref_table}({ref_column})"
            )
            statements.append(fk_stmt)

    return statements, errors


def prompt_and_apply_updates(conn, all_updates: Dict[str, Any]) -> Any:
    """
    Show schema differences and ask user for confirmation before applying.

    Args:
        conn: PostgreSQL connection object
        all_updates: Dict mapping table names to their update info

    Returns:
        The connection object
    """
    print("\n" + "="*70)
    print("SCHEMA DIFFERENCES DETECTED")
    print("="*70 + "\n")

    # Display all differences
    all_statements = []
    all_errors = []

    for table_name, update_info in all_updates.items():
        differences = update_info['differences']
        statements, errors = generate_alter_statements(table_name, differences, conn)

        print(f"\n📋 Table: {table_name}")
        print(f"   Status: {differences['status']}")

        if differences['added_columns']:
            print(f"   Columns to add: {len(differences['added_columns'])}")
            for col in differences['added_columns']:
                nullable_str = "NULL" if col.nullable else "NOT NULL"
                fk_str = f" -> {col.foreign_key}" if col.foreign_key else ""
                print(f"      - {col.name} {col.sql_type} {nullable_str}{fk_str}")

        if statements:
            print(f"\n   SQL statements:")
            for stmt in statements:
                print(f"      {stmt}")
                all_statements.append(stmt)

        if errors:
            print(f"\n   ⚠️  Errors:")
            for error in errors:
                print(f"      ❌ {error['message']}")
                print(f"         Reason: {error['reason']}")
                print(f"         Solution: {error['solution']}")
                all_errors.append(error)

    print("\n" + "="*70)

    # If there are blocking errors, don't allow proceeding
    if all_errors:
        print("❌ Cannot proceed due to schema conflicts.")
        print("   Please resolve the errors above before migrating.\n")
        return conn

    if not all_statements:
        print("✅ No schema updates needed.\n")
        return conn

    # Ask for confirmation
    print(f"\nFound {len(all_statements)} ALTER TABLE statement(s) to execute.")
    response = input("Apply these schema updates? (yes/no): ").strip().lower()

    if response in ['yes', 'y']:
        cursor = conn.cursor()
        for stmt in all_statements:
            try:
                print(f"Executing: {stmt}")
                cursor.execute(stmt)
                print(f"   ✅ Success")
            except Exception as e:
                print(f"   ❌ Error: {e}")
                conn.rollback()
                return conn

        conn.commit()
        print("\n✅ Schema updates applied successfully\n")
    else:
        print("\n❌ Schema updates skipped\n")

    return conn
