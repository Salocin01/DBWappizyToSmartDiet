from mongo_connection import get_mongo_collection
from postgres_connection import connect_postgres, setup_tables
from schemas import TABLE_SCHEMAS
from data_export import export_table_data, print_import_summary
from import_summary import ImportSummary


if __name__ == "__main__":
    conn = connect_postgres()
    conn = setup_tables(conn)
    
    # Sort tables by export_order
    sorted_tables = sorted(TABLE_SCHEMAS.items(), key=lambda x: x[1].export_order)
    
    for table_name, schema in sorted_tables:
        collection = get_mongo_collection(schema.mongo_collection)
        
        custom_filter = None
        
        # Create a separate summary instance for each entity
        entity_summary = ImportSummary()
        export_table_data(conn, table_name, collection, custom_filter, entity_summary)
        print_import_summary(table_name, entity_summary)
    
    conn.close()
    print("✅ All data migration completed successfully!")