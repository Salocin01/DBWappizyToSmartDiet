from datetime import datetime
import os
import psycopg2

from src.migration.import_summary import ImportSummary


class PostgresRepository:
    def __init__(self, conn, summary_instance=None, import_by_batch=True, direct_import=True):
        self.conn = conn
        self.summary = summary_instance or ImportSummary()
        self.import_by_batch = import_by_batch
        self.direct_import = direct_import

    def execute_batch(
        self,
        batch_values,
        columns,
        table_name,
        use_on_conflict=False,
        on_conflict_clause=None,
    ):
        if self.direct_import:
            return self._execute_direct_sql(
                batch_values,
                columns,
                table_name,
                use_on_conflict,
                on_conflict_clause,
            )

        return self.write_sql_file(
            batch_values,
            columns,
            table_name,
            use_on_conflict,
            on_conflict_clause,
        )

    def _execute_direct_sql(
        self,
        batch_values,
        columns,
        table_name,
        use_on_conflict=False,
        on_conflict_clause=None,
    ):
        if not batch_values or not columns:
            return 0

        batch_values = [values for values in batch_values if values and len(values) > 0]
        if not batch_values:
            return 0

        placeholders = ", ".join(["%s"] * len(columns))
        conflict_clause = (
            on_conflict_clause
            if on_conflict_clause is not None
            else (" ON CONFLICT (id) DO NOTHING" if use_on_conflict else "")
        )
        sql_template = (
            f"INSERT INTO {table_name} ({', '.join(columns)}) "
            f"VALUES ({placeholders}){conflict_clause}"
        )

        cursor = self.conn.cursor()
        try:
            if self.import_by_batch:
                cursor.execute("SAVEPOINT batch_insert")
                try:
                    cursor.executemany(sql_template, batch_values)
                    actual_insertions = cursor.rowcount

                    if not use_on_conflict and actual_insertions != len(batch_values):
                        cursor.execute("ROLLBACK TO SAVEPOINT batch_insert")
                        return self._handle_batch_errors(
                            cursor, sql_template, batch_values, table_name
                        )

                    cursor.execute("RELEASE SAVEPOINT batch_insert")
                    skipped_count = len(batch_values) - actual_insertions
                    self.summary.record_success(table_name, actual_insertions)
                    if skipped_count > 0:
                        self.summary.record_skipped(table_name, skipped_count)

                    self.conn.commit()
                    return actual_insertions
                except psycopg2.IntegrityError:
                    cursor.execute("ROLLBACK TO SAVEPOINT batch_insert")
                    return self._handle_batch_errors(
                        cursor, sql_template, batch_values, table_name
                    )
            else:
                successful_count = 0
                for values in batch_values:
                    cursor.execute("SAVEPOINT individual_insert")
                    try:
                        cursor.execute(sql_template, values)
                        cursor.execute("RELEASE SAVEPOINT individual_insert")
                        self.summary.record_success(table_name)
                        successful_count += 1
                    except psycopg2.IntegrityError as e:
                        cursor.execute("ROLLBACK TO SAVEPOINT individual_insert")
                        self._record_integrity_error(table_name, e, values)
                        continue

                self.conn.commit()
                return successful_count
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def _handle_batch_errors(self, cursor, sql, batch_values, table_name):
        cursor.close()
        cursor = self.conn.cursor()
        successful_count = 0

        for values in batch_values:
            cursor.execute("SAVEPOINT individual_retry")
            try:
                cursor.execute(sql, values)
                cursor.execute("RELEASE SAVEPOINT individual_retry")
                self.summary.record_success(table_name)
                successful_count += 1
            except psycopg2.IntegrityError as individual_e:
                cursor.execute("ROLLBACK TO SAVEPOINT individual_retry")
                self._record_integrity_error(table_name, individual_e, values)
                continue
            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT individual_retry")
                failed_record = {"id": values[0] if values else "unknown", "values": values}
                self.summary.record_error(
                    table_name, f"Unexpected error: {str(e)[:100]}", failed_record
                )
                continue

        self.conn.commit()
        cursor.close()
        return successful_count

    def _record_integrity_error(self, table_name, error, values):
        error_message = str(error).lower()
        failed_record = {"id": values[0] if values else "unknown", "values": values}

        if "foreign key constraint" in error_message:
            self.summary.record_error(table_name, "Foreign key constraint", failed_record)
        elif "null value" in error_message or "not-null constraint" in error_message:
            self.summary.record_error(table_name, "NULL constraint", failed_record)
        else:
            self.summary.record_error(
                table_name, f"Other integrity error: {str(error)[:100]}", failed_record
            )

    def execute_sql_file(self, sql_file_path):
        if not os.path.exists(sql_file_path):
            return 0

        cursor = self.conn.cursor()
        executed_count = 0
        failed_count = 0

        try:
            with open(sql_file_path, "r", encoding="utf-8") as f:
                sql_content = f.read()

            statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

            for i, statement in enumerate(statements):
                cursor.execute("SAVEPOINT sql_statement")
                try:
                    cursor.execute(statement)
                    cursor.execute("RELEASE SAVEPOINT sql_statement")
                    executed_count += 1
                except psycopg2.IntegrityError as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT sql_statement")
                    failed_count += 1
                    table_name = self._extract_table_name(statement)
                    self.summary.record_error(
                        table_name,
                        f"SQL file integrity error: {str(e)[:100]}",
                        {"statement_index": i},
                    )
                    continue
                except Exception as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT sql_statement")
                    failed_count += 1
                    table_name = self._extract_table_name(statement)
                    self.summary.record_error(
                        table_name,
                        f"SQL file execution error: {str(e)[:100]}",
                        {"statement_index": i},
                    )
                    continue

            self.conn.commit()
            print(
                f"SQL file execution completed: {executed_count} successful, {failed_count} failed"
            )
            return executed_count
        except Exception as e:
            print(f"Error reading SQL file {sql_file_path}: {e}")
            self.conn.rollback()
            return 0
        finally:
            cursor.close()

    def write_sql_file(
        self, batch_values, columns, table_name, use_on_conflict=False, on_conflict_clause=None
    ):
        if not batch_values or not columns:
            return 0

        conflict_clause = (
            on_conflict_clause
            if on_conflict_clause is not None
            else (" ON CONFLICT (id) DO NOTHING" if use_on_conflict else "")
        )

        os.makedirs("sql_exports", exist_ok=True)
        sql_file_path = f"sql_exports/{table_name}_import.sql"

        with open(sql_file_path, "a", encoding="utf-8") as f:
            for values in batch_values:
                formatted_values = []
                for value in values:
                    if value is None:
                        formatted_values.append("NULL")
                    elif isinstance(value, str):
                        escaped_value = value.replace("'", "''")
                        formatted_values.append(f"'{escaped_value}'")
                    elif isinstance(value, datetime):
                        formatted_values.append(f"'{value.isoformat()}'")
                    else:
                        formatted_values.append(str(value))

                sql_statement = (
                    f"INSERT INTO {table_name} ({', '.join(columns)}) "
                    f"VALUES ({', '.join(formatted_values)}){conflict_clause};\n"
                )
                f.write(sql_statement)

        self.summary.record_success(table_name, len(batch_values))
        print(f"Generated SQL for {len(batch_values)} records in {sql_file_path}")
        return len(batch_values)

    def delete_by_parent_ids(self, table_name, column_name, parent_ids):
        if not parent_ids:
            return 0

        cursor = self.conn.cursor()
        try:
            placeholders = ", ".join(["%s"] * len(parent_ids))
            delete_sql = f"DELETE FROM {table_name} WHERE {column_name} IN ({placeholders})"
            cursor.execute(delete_sql, parent_ids)
            deleted_count = cursor.rowcount
            self.conn.commit()
            return deleted_count
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def _extract_table_name(self, statement):
        table_name = "unknown"
        if "INSERT INTO" in statement.upper():
            try:
                table_name = statement.upper().split("INSERT INTO")[1].split()[0]
            except Exception:
                pass
        return table_name
