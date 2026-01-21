from src.connections.mongo_connection import get_mongo_collection, MongoConnection
from src.connections.postgres_connection import connect_postgres, setup_tables, close_postgres_connection
from src.schemas.schemas import TABLE_SCHEMAS
from src.migration.data_export import export_table_data, get_last_insert_date, print_import_summary
from src.migration.import_summary import ImportSummary


def run_migration():
    try:
        conn = connect_postgres()
        conn = setup_tables(conn)

        # Sort tables by export_order to respect foreign key dependencies
        sorted_tables = sorted(TABLE_SCHEMAS.items(), key=lambda x: x[1].export_order)

        for table_name, schema in sorted_tables:
            print(f"\n{'='*80}")
            print(f"Processing table: {table_name}")
            print(f"{'='*80}")

            collection = get_mongo_collection(schema.mongo_collection)
            entity_summary = ImportSummary()

            # Check for forced reimport
            if schema.force_reimport:
                print("🔄 FORCE REIMPORT enabled for this table")
                if schema.truncate_before_import:
                    print("⚠️  TRUNCATE enabled - clearing all existing data")
                    cursor = conn.cursor()
                    try:
                        cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                        conn.commit()
                        print(f"   → Table {table_name} truncated successfully")
                    except Exception as e:
                        print(f"   ⚠️ Error truncating table: {e}")
                        conn.rollback()
                    finally:
                        cursor.close()
                after_date = None
                print("   → Will perform full reimport from MongoDB")
            else:
                # STEP 1: Get Last Migration Date from PostgreSQL
                after_date = get_last_insert_date(conn, table_name)
                if after_date:
                    print(f"📅 Step 1: Last migration date: {after_date}")
                    print("   → Will import records created or updated after this date")
                else:
                    print("📅 Step 1: No existing records found")
                    print("   → Will perform full import")

            # STEP 2-4: Strategy handles fetching, transforming, and importing
            export_table_data(
                conn,
                table_name=table_name,
                collection=collection,
                summary_instance=entity_summary,
                after_date=after_date,
            )

            print_import_summary(table_name, entity_summary)

        print("\n" + "=" * 80)
        print("✅ All data migration completed successfully!")
        print("=" * 80)

    finally:
        # Close connections and SSH tunnels
        if 'conn' in locals():
            conn.close()

        # Close MongoDB connection and SSH tunnel
        mongo_conn = MongoConnection()
        mongo_conn.close()

        # Close PostgreSQL SSH tunnel
        close_postgres_connection()
