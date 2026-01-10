from dataclasses import dataclass
from typing import Dict, List, Optional, Any

@dataclass
class ColumnDefinition:
    name: str
    sql_type: str
    nullable: bool = True
    primary_key: bool = False
    foreign_key: Optional[str] = None

@dataclass
class TableSchema:
    name: str
    mongo_collection: str
    columns: List[ColumnDefinition]
    field_mappings: Dict[str, str]
    export_order: int = 0
    import_strategy: Optional[Any] = None
    unique_constraints: Optional[List[List[str]]] = None
    
    @classmethod
    def create(cls, columns: List[ColumnDefinition], name: Optional[str] = None,
               mongo_collection: Optional[str] = None, explicit_mappings: Optional[Dict[str, str]] = None, 
               export_order: int = 0, import_strategy: Optional[Any] = None, 
               unique_constraints: Optional[List[List[str]]] = None) -> 'TableSchema':
        """Create a TableSchema with auto-generated field mappings.
        
        Args:
            columns: List of column definitions
            name: Table name (will be set from schema key if not specified)
            mongo_collection: MongoDB collection name (defaults to table name if not specified)
            explicit_mappings: Only specify mappings where column name differs from field name
        """
        # Note: name will be set from schema key in TABLE_SCHEMAS if not provided
        # Use table name as MongoDB collection name if not specified
        if mongo_collection is None:
            mongo_collection = name
        # Start with auto-generated mappings, but exclude 'id' if it exists (handled by _id mapping)
        # Also exclude columns that have explicit mappings to avoid duplicates
        excluded_columns = {'id'}
        if explicit_mappings:
            excluded_columns.update(explicit_mappings.values())
        
        field_mappings = {col.name: col.name for col in columns if col.name not in excluded_columns}
        
        # Override with explicit mappings if provided
        if explicit_mappings:
            field_mappings.update(explicit_mappings)
            
        return cls(name, mongo_collection, columns, field_mappings, export_order, import_strategy, unique_constraints)
    
    def get_create_sql(self) -> str:
        column_defs = []
        foreign_keys = []
        unique_constraints = []
        
        for col in self.columns:
            col_def = f"{col.name} {col.sql_type}"
            if col.primary_key:
                col_def += " PRIMARY KEY"
            elif not col.nullable:
                col_def += " NOT NULL"
            column_defs.append(col_def)
            
            if col.foreign_key:
                foreign_keys.append(f"FOREIGN KEY ({col.name}) REFERENCES {col.foreign_key}")
        
        if self.unique_constraints:
            for constraint in self.unique_constraints:
                constraint_cols = ', '.join(constraint)
                unique_constraints.append(f"UNIQUE ({constraint_cols})")
        
        all_defs = column_defs + foreign_keys + unique_constraints
        return f"""
        CREATE TABLE IF NOT EXISTS {self.name} (
            {',\n            '.join(all_defs)}
        );
        """
    
    def get_on_conflict_clause(self, columns: list = None) -> str:
        """Get the appropriate ON CONFLICT clause for this table

        Args:
            columns: Optional list of columns being inserted. If provided, only these columns
                    will be included in the UPDATE clause. If None, all schema columns are used.
        """
        # Determine which columns to use for UPDATE clause
        if columns:
            # Use only the columns being inserted
            insert_columns = set(columns)
        else:
            # Use all schema columns
            insert_columns = {col.name for col in self.columns}

        # Check if table has an id column (primary key)
        id_column = next((col for col in self.columns if col.name == 'id' and col.primary_key), None)
        if id_column:
            # Build UPDATE SET clause for all columns except id and primary keys
            update_columns = []
            for col in self.columns:
                if not col.primary_key and col.name in insert_columns:
                    update_columns.append(f"{col.name} = EXCLUDED.{col.name}")

            if update_columns:
                update_clause = ', '.join(update_columns)
                return f" ON CONFLICT (id) DO UPDATE SET {update_clause}"
            else:
                return " ON CONFLICT (id) DO NOTHING"

        # Check if table has unique constraints
        if self.unique_constraints:
            # Use the first unique constraint
            constraint_cols = ', '.join(self.unique_constraints[0])

            # Build UPDATE SET clause for all columns except those in unique constraint
            update_columns = []
            constraint_set = set(self.unique_constraints[0])
            for col in self.columns:
                if col.name not in constraint_set and not col.primary_key and col.name in insert_columns:
                    update_columns.append(f"{col.name} = EXCLUDED.{col.name}")

            if update_columns:
                update_clause = ', '.join(update_columns)
                return f" ON CONFLICT ({constraint_cols}) DO UPDATE SET {update_clause}"
            else:
                return f" ON CONFLICT ({constraint_cols}) DO NOTHING"

        # No appropriate conflict resolution found
        return ""


class BaseEntitySchema:
    """Base class for entity schemas with common columns and mappings"""
    
    @classmethod
    def get_base_columns(cls) -> List[ColumnDefinition]:
        """Returns the standard columns that all entities should have"""
        return [
            ColumnDefinition('id', 'VARCHAR', primary_key=True),
            ColumnDefinition('created_at', 'TIMESTAMP', nullable=False),
            ColumnDefinition('updated_at', 'TIMESTAMP', nullable=False)
        ]
    
    @classmethod
    def get_base_mappings(cls) -> Dict[str, str]:
        """Returns the standard field mappings that all entities should have"""
        return {
            '_id': 'id',
            'creation_date': 'created_at',
            'update_date': 'updated_at'
        }
    
    @classmethod
    def create_with_base(cls, additional_columns: List[ColumnDefinition] = None, 
                        name: Optional[str] = None, mongo_collection: Optional[str] = None,
                        additional_mappings: Optional[Dict[str, str]] = None, 
                        export_order: int = 0, import_strategy: Optional[Any] = None) -> TableSchema:
        """Create a TableSchema with base columns and mappings plus additional ones.
        
        Args:
            additional_columns: Additional columns beyond the base ones
            name: Table name (will be set from schema key if not specified)
            mongo_collection: MongoDB collection name (defaults to table name if not specified)
            additional_mappings: Additional mappings beyond the base ones
            export_order: Export order for the table
            import_strategy: Import strategy for the table
        """
        # Combine base columns with additional columns
        columns = cls.get_base_columns()
        if additional_columns:
            columns.extend(additional_columns)
        
        # Combine base mappings with additional mappings
        explicit_mappings = cls.get_base_mappings()
        if additional_mappings:
            explicit_mappings.update(additional_mappings)
        
        return TableSchema.create(
            columns=columns,
            name=name,
            mongo_collection=mongo_collection,
            explicit_mappings=explicit_mappings,
            export_order=export_order,
            import_strategy=import_strategy
        )

