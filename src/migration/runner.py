from src.connections.mongo_connection import get_mongo_collection, MongoConnection
from src.connections.postgres_connection import connect_postgres, setup_tables, close_postgres_connection, parse_global_date_threshold
from src.schemas.schemas import TABLE_SCHEMAS
from src.migration.data_export import export_table_data, get_last_insert_date, print_import_summary
from src.migration.import_summary import ImportSummary
from datetime import datetime
from typing import Optional


def apply_global_threshold(table_date: Optional[datetime],
                          global_threshold: Optional[datetime]) -> Optional[datetime]:
    """
    Determine the effective migration start date using the earlier of two dates.

    Logic:
    - If only table_date exists: use table_date
    - If only global_threshold exists: use global_threshold
    - If both exist: use earlier date (extends sync window backward)
    - If neither exists: use None (full migration)

    Args:
        table_date: Last migration date from PostgreSQL (table-specific)
        global_threshold: Global threshold from environment variable

    Returns:
        Effective after_date to use for filtering
    """
    if global_threshold is None:
        return table_date

    if table_date is None:
        return global_threshold

    # Both dates exist - use the earlier one
    effective_date = min(table_date, global_threshold)

    if effective_date == global_threshold:
        print(f"   → Global threshold is earlier; extending sync window backward")

    return effective_date


def run_migration():
    try:
        conn = connect_postgres()
        conn = setup_tables(conn)

        # Load global threshold once at migration start
        global_threshold = parse_global_date_threshold()
        if global_threshold:
            print(f"\n🌐 Global date threshold active: {global_threshold.strftime('%Y-%m-%d')}")
            print()

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
                print("   → Global date threshold bypassed")
                print("   → Will perform full reimport from MongoDB")
            else:
                # STEP 1: Get Last Migration Date from PostgreSQL
                table_last_date = get_last_insert_date(conn, table_name)

                # Apply global threshold logic (use earlier date)
                after_date = apply_global_threshold(table_last_date, global_threshold)

                # Enhanced logging
                if after_date:
                    print(f"📅 Step 1: Last migration date: {after_date.strftime('%Y-%m-%d %H:%M:%S')}")
                    if global_threshold and after_date == global_threshold:
                        print(f"   → Using global threshold (earlier than table date)")
                    else:
                        print("   → Will import records created or updated after this date")
                else:
                    print("📅 Step 1: No existing records found")
                    if global_threshold:
                        print(f"   → Will use global threshold: {global_threshold.strftime('%Y-%m-%d')}")
                    else:
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
