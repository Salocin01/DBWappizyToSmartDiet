from src.schemas.schemas import TABLE_SCHEMAS
from bson import ObjectId
import psycopg2
from src.connections.mongo_connection import get_mongo_collection
from .import_summary import ImportSummary

# Global instance for backward compatibility
import_summary = ImportSummary()

def get_last_insert_date(conn, table_name):
    """Get the latest created_at or updated_at date from a table to use as starting point for incremental imports"""
    cursor = conn.cursor()
    try:
        # Get the maximum of both created_at and updated_at to catch both new and updated records
        cursor.execute(f"""
            SELECT GREATEST(
                COALESCE(MAX(created_at), '1900-01-01'::timestamp), 
                COALESCE(MAX(updated_at), '1900-01-01'::timestamp)
            ) FROM {table_name}
        """)
        result = cursor.fetchone()
        # Return None if the result is the default '1900-01-01' date
        last_date = result[0] if result and result[0] else None
        if last_date and str(last_date) == '1900-01-01 00:00:00':
            return None
        return last_date
    except psycopg2.Error as e:
        print(f"Error getting last insert date for {table_name}: {e}")
        return None
    finally:
        cursor.close()

def print_import_summary(entities=None, summary_instance=None):
    """Print a summary of import statistics by entity
    
    Args:
        entities: String or list of entity names to show. If None, shows all.
        summary_instance: ImportSummary instance to use. If None, uses global instance.
    """
    summary = summary_instance or import_summary
    summary.print_summary(entities)

def export_table_data(conn, table_name, collection, custom_filter=None, summary_instance=None, after_date=None, batch_size=5000):
    from .import_strategies import ImportConfig, DirectTranslationStrategy
    
    schema = TABLE_SCHEMAS[table_name]
    
    # Create import configuration
    config = ImportConfig(
        table_name=table_name,
        source_collection=collection.name,
        batch_size=batch_size,
        after_date=after_date,
        custom_filter=custom_filter,
        summary_instance=summary_instance
    )
    
    # Use strategy from schema or default to DirectTranslationStrategy
    strategy = getattr(schema, 'import_strategy', None) or DirectTranslationStrategy()
    
    return strategy.export_data(conn, collection, config)




