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
    
    @classmethod
    def create(cls, columns: List[ColumnDefinition], name: Optional[str] = None,
               mongo_collection: Optional[str] = None, explicit_mappings: Optional[Dict[str, str]] = None, 
               export_order: int = 0, import_strategy: Optional[Any] = None) -> 'TableSchema':
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
            
        return cls(name, mongo_collection, columns, field_mappings, export_order, import_strategy)
    
    def get_create_sql(self) -> str:
        column_defs = []
        foreign_keys = []
        
        for col in self.columns:
            col_def = f"{col.name} {col.sql_type}"
            if col.primary_key:
                col_def += " PRIMARY KEY"
            elif not col.nullable:
                col_def += " NOT NULL"
            column_defs.append(col_def)
            
            if col.foreign_key:
                foreign_keys.append(f"FOREIGN KEY ({col.name}) REFERENCES {col.foreign_key}")
        
        all_defs = column_defs + foreign_keys
        return f"""
        CREATE TABLE IF NOT EXISTS {self.name} (
            {',\n            '.join(all_defs)}
        );
        """


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

