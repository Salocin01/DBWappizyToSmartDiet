from src.connections.mongo_connection import get_mongo_collection
from src.connections.postgres_connection import connect_postgres, setup_tables
from src.schemas.schemas import TABLE_SCHEMAS
from src.migration.data_export import export_table_data, get_last_insert_date, print_import_summary
from src.migration.import_summary import ImportSummary


def run_migration():
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

    conn.close()
    print("\n" + "=" * 80)
    print("✅ All data migration completed successfully!")
    print("=" * 80)
