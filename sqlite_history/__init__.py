from .sql import backfill_sql, history_table_sql, triggers_sql, table_columns_and_types


def configure_history(db, table):
    """Configure history triggers for a table in a database."""
    # Get the table schema
    columns_and_types = table_columns_and_types(db, table)
    columns = [r[0] for r in columns_and_types]
    with db:
        db.executescript(history_table_sql(table, columns_and_types))
        # Create the triggers
        db.executescript(triggers_sql(table, columns))
        # Backfill the history table
        db.execute(backfill_sql(table, columns))
