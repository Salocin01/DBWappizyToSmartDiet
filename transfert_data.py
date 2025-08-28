from mongo_connection import get_mongo_collection
from postgres_connection import connect_postgres, setup_tables
from schemas import TABLE_SCHEMAS
from data_export import export_table_data, print_import_summary, get_last_insert_date
from import_summary import ImportSummary


if __name__ == "__main__":
    conn = connect_postgres()
    conn = setup_tables(conn)
    
    # Sort tables by export_order
    sorted_tables = sorted(TABLE_SCHEMAS.items(), key=lambda x: x[1].export_order)
    
    for table_name, schema in sorted_tables:
        collection = get_mongo_collection(schema.mongo_collection)
        
        custom_filter = None
        after_date = None
        
        # Get the last insert date to only import newer records for all tables
        after_date = get_last_insert_date(conn, table_name)
        if after_date:
            print(f"📅 Importing {table_name} created after {after_date}")
        else:
            print(f"📅 No existing records found in {table_name}, importing all")
        
        # Create a separate summary instance for each entity
        entity_summary = ImportSummary()
        export_table_data(conn, table_name, collection, custom_filter, entity_summary, after_date=after_date)
        print_import_summary(table_name, entity_summary)
    
    conn.close()
    print("✅ All data migration completed successfully!")