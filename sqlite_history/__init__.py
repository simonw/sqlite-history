from .sql import backfill_sql, history_table_sql, triggers_sql


def configure_history(db, table):
    """Configure history triggers for a table in a database."""
    # Get the table schema
    cursor = db.execute(f"PRAGMA table_info({table});")
    columns_and_types = []
    columns = []
    for row in cursor.fetchall():
        columns_and_types.append((row[1], row[2]))
        columns.append(row[1])
    # Create the history table
    with db:
        db.executescript(history_table_sql(table, columns_and_types))
        # Create the triggers
        db.executescript(triggers_sql(table, columns))
        # Backfill the history table
        db.execute(backfill_sql(table, columns))
