import argparse
import sqlite3
import os
from . import configure_history


def configure_triggers(database_path, tables):
    db = sqlite3.connect(database_path)
    for table in tables:
        if table.startswith("_") and table.endswith("_history"):
            continue
        # Does a history table exist already?
        history_table_name = f"_{table}_history"
        cursor = db.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{history_table_name}';"
        )
        if cursor.fetchone():
            print(f"History table {history_table_name} already exists - skipping.")
            continue
        configure_history(db, table)


def get_all_table_names(database_path):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' and sql not like '%virtual table%';"
    )
    return [table[0] for table in cursor.fetchall()]


def run():
    parser = argparse.ArgumentParser(
        description="Configure sqlite-history triggers for one or more tables."
    )

    parser.add_argument("database_path", help="Path to the SQLite database file.")
    parser.add_argument(
        "tables", nargs="*", help="One or more table names to configure."
    )
    parser.add_argument(
        "--all", action="store_true", help="Configure for all tables in the database."
    )

    args = parser.parse_args()

    if args.all:
        args.tables = get_all_table_names(args.database_path)
    elif len(args.tables) == 0:
        parser.error(
            "No tables provided. Please provide table names or use --all flag."
        )

    # Error if database_path doesn't exist
    if not os.path.exists(args.database_path):
        parser.error("Database file does not exist.")

    # Error if any of the tables don't exist
    all_table_names = get_all_table_names(args.database_path)
    missing_tables = [table for table in args.tables if table not in all_table_names]
    if missing_tables:
        parser.error("The following tables do not exist: " + ", ".join(missing_tables))

    configure_triggers(args.database_path, args.tables)


if __name__ == "__main__":
    run()
