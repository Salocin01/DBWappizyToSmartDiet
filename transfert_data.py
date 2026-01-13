from src.connections.mongo_connection import get_mongo_collection
from src.connections.postgres_connection import connect_postgres, setup_tables
from src.schemas.schemas import TABLE_SCHEMAS
from src.migration.data_export import print_import_summary, get_last_insert_date
from src.migration.import_summary import ImportSummary
from src.migration.import_strategies import ImportConfig, DirectTranslationStrategy


if __name__ == "__main__":
    conn = connect_postgres()
    conn = setup_tables(conn)

    # Sort tables by export_order to respect foreign key dependencies
    sorted_tables = sorted(TABLE_SCHEMAS.items(), key=lambda x: x[1].export_order)

    # ===========================================================================
    # MIGRATION LOOP - Process each table using the 4-step pattern
    # ===========================================================================
    for table_name, schema in sorted_tables:
        print(f"\n{'='*80}")
        print(f"Processing table: {table_name}")
        print(f"{'='*80}")

        collection = get_mongo_collection(schema.mongo_collection)
        entity_summary = ImportSummary()

        # =======================================================================
        # STEP 1: Get Last Migration Date from PostgreSQL
        # =======================================================================
        # Query PostgreSQL for the latest created_at/updated_at timestamp
        # Returns None if table is empty (triggers full import)
        # Uses GREATEST(MAX(created_at), MAX(updated_at)) to capture all changes
        after_date = get_last_insert_date(conn, table_name)
        if after_date:
            print(f"📅 Step 1: Last migration date: {after_date}")
            print(f"   → Will import records created or updated after this date")
        else:
            print(f"📅 Step 1: No existing records found")
            print(f"   → Will perform full import")

        # Get the import strategy for this table
        strategy = getattr(schema, 'import_strategy', None) or DirectTranslationStrategy()

        # Create import configuration
        config = ImportConfig(
            table_name=table_name,
            source_collection=schema.mongo_collection,
            batch_size=5000,
            after_date=after_date,
            custom_filter=None,
            summary_instance=entity_summary
        )

        # =======================================================================
        # CHECK: Does strategy have custom export_data implementation?
        # =======================================================================
        # Some strategies (like UsersLogbooksMomentsDetailsStrategy) override
        # export_data to use pre-computed data and should bypass normal flow
        strategy_class = strategy.__class__
        uses_custom_export = (
            hasattr(strategy_class, 'export_data') and
            strategy_class.export_data is not strategy_class.__bases__[0].export_data
        )

        if uses_custom_export:
            print(f"📦 Strategy uses custom export_data - calling directly...")
            total_records_processed = strategy.export_data(conn, collection, config)
            print(f"✅ Completed: {total_records_processed} total records processed for {table_name}")
            print_import_summary(table_name, entity_summary)
            continue

        # =======================================================================
        # STEP 2: Query New/Updated Documents from MongoDB
        # =======================================================================
        # Count documents matching the filter criteria
        # Filter: {$or: [{creation_date: {$gte: after_date}}, {update_date: {$gte: after_date}}]}
        # Plus any strategy-specific filters (e.g., array existence checks)
        total_documents = strategy.count_total_documents(collection, config)
        print(f"📊 Step 2: Querying new/updated documents from MongoDB")
        print(f"   → Found {total_documents} documents to process")
        if total_documents == 0:
            print(f"   → No changes detected, skipping this table")
            continue

        # =======================================================================
        # STEP 3: Transform Data for PostgreSQL
        # =======================================================================
        # Fetch documents in batches and transform each to SQL format
        # - strategy.get_documents() - fetches paginated batches
        # - strategy.extract_data_for_sql() - transforms each document
        # - Handles ObjectId → string conversion
        # - Maps MongoDB fields to PostgreSQL columns
        print(f"🔄 Step 3: Transforming documents to SQL format...")

        total_records_processed = 0
        offset = 0
        batch_size = config.batch_size

        while True:
            # Fetch a batch of documents from MongoDB
            documents = strategy.get_documents(collection, config, offset=offset)

            if not documents:
                break

            print(f"   → Processing batch: {offset} to {offset + len(documents)} of {total_documents}")

            # Reset skip counter for this batch
            config._skip_count = {}

            # Transform documents to SQL format
            all_batch_values = []
            columns = None
            parent_ids_for_deletion = []  # For DeleteAndInsertStrategy

            for document in documents:
                # Extract SQL values from document
                values, doc_columns = strategy.extract_data_for_sql(document, config)

                if values is not None:
                    if columns is None:
                        columns = doc_columns

                    # Collect parent IDs for DeleteAndInsertStrategy
                    if hasattr(strategy, 'get_parent_id_from_document'):
                        parent_id = strategy.get_parent_id_from_document(document)
                        parent_ids_for_deletion.append(parent_id)

                    # Handle both single row and multiple rows per document
                    if isinstance(values, list) and len(values) > 0 and isinstance(values[0], list):
                        all_batch_values.extend(values)
                    else:
                        all_batch_values.append(values)

            if not all_batch_values:
                # Print skip statistics if available
                if hasattr(config, '_skip_count') and config._skip_count:
                    print(f"   ⚠️  All {len(documents)} documents filtered out. Skip reasons:")
                    for reason, count in sorted(config._skip_count.items(), key=lambda x: -x[1]):
                        print(f"       - {reason}: {count}")
                offset += batch_size
                continue

            print(f"   → Transformed {len(documents)} documents into {len(all_batch_values)} SQL rows")

            # Print skip statistics if any documents were skipped
            if hasattr(config, '_skip_count') and config._skip_count:
                skipped_count = sum(config._skip_count.values())
                print(f"   ℹ️  Skipped {skipped_count} documents. Skip reasons:")
                for reason, count in sorted(config._skip_count.items(), key=lambda x: -x[1]):
                    print(f"       - {reason}: {count}")

            # =======================================================================
            # STEP 4: Execute Import to PostgreSQL
            # =======================================================================
            # Strategy-specific import behavior:
            #   - DirectTranslationStrategy: ON CONFLICT DO UPDATE (upsert)
            #   - ArrayExtractionStrategy: ON CONFLICT DO UPDATE (upsert)
            #   - DeleteAndInsertStrategy: DELETE old + INSERT fresh (for relationships)
            print(f"💾 Step 4: Executing import to PostgreSQL...")

            # For DeleteAndInsertStrategy: Delete existing relationships first
            if hasattr(strategy, 'get_delete_column_name') and parent_ids_for_deletion:
                print(f"   → DELETE-AND-INSERT pattern detected")
                cursor = conn.cursor()
                try:
                    delete_table = strategy.get_delete_table_name(config)
                    delete_column = strategy.get_delete_column_name()
                    placeholders = ', '.join(['%s'] * len(parent_ids_for_deletion))
                    delete_sql = f"DELETE FROM {delete_table} WHERE {delete_column} IN ({placeholders})"

                    cursor.execute(delete_sql, parent_ids_for_deletion)
                    deleted_count = cursor.rowcount
                    conn.commit()
                    print(f"   → Deleted {deleted_count} existing relationships for {len(parent_ids_for_deletion)} parents")
                except Exception as e:
                    print(f"   ⚠️  Error during DELETE: {e}")
                    conn.rollback()
                finally:
                    cursor.close()

            # Insert/Update records using the strategy's conflict resolution
            from src.migration.import_strategies import ImportUtils

            use_on_conflict = strategy.get_use_on_conflict()
            on_conflict_clause = strategy.get_on_conflict_clause(table_name, columns) if use_on_conflict else ""

            if use_on_conflict:
                print(f"   → Using UPSERT (ON CONFLICT DO UPDATE)")
            else:
                print(f"   → Using INSERT (fresh data)")

            actual_insertions = ImportUtils.execute_batch(
                conn,
                all_batch_values,
                columns,
                table_name,
                entity_summary,
                use_on_conflict=use_on_conflict,
                on_conflict_clause=on_conflict_clause
            )

            total_records_processed += actual_insertions
            print(f"   → Inserted/Updated {actual_insertions} records")

            # Move to next batch
            offset += batch_size

            if len(documents) < batch_size:
                break

        print(f"✅ Completed: {total_records_processed} total records processed for {table_name}")

        # Print summary for this table
        print_import_summary(table_name, entity_summary)

    conn.close()
    print("\n" + "="*80)
    print("✅ All data migration completed successfully!")
    print("="*80)