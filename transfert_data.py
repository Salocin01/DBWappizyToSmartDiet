from mongo_connection import get_mongo_collection
from postgres_connection import connect_postgres, setup_tables
from schemas import TABLE_SCHEMAS
from data_export import export_table_data


if __name__ == "__main__":
    conn = connect_postgres()
    conn = setup_tables(conn)
    
    # Sort tables by export_order
    sorted_tables = sorted(TABLE_SCHEMAS.items(), key=lambda x: x[1].export_order)
    
    for table_name, schema in sorted_tables:
        collection = get_mongo_collection(schema.mongo_collection)
        
        custom_filter = None
        
        export_table_data(conn, table_name, collection, custom_filter)
    
    conn.close()
    print("✅ All data migration completed successfully!")