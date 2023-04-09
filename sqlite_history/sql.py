import re


def history_table_sql(table, columns_and_types):
    """Return SQL for history table for a table and its columns."""
    if isinstance(columns_and_types, dict):
        columns_and_types = dict.items()
    column_names = ",\n".join(
        "   {name} {type}".format(name=escape_sqlite(name), type=type)
        for name, type in columns_and_types
    )
    return """
CREATE TABLE _{table}_history (
    _rowid INTEGER,
{column_names},
    _version INTEGER,
    _updated INTEGER,
    _mask INTEGER
);
CREATE INDEX idx_{table}_history_rowid ON _{table}_history (_rowid);
""".format(
        table=table, column_names=column_names
    )


def triggers_sql(table, columns):
    """Return SQL for triggers for a table and its columns."""
    column_names = ", ".join(escape_sqlite(column) for column in columns)
    new_column_values = ", ".join("new." + escape_sqlite(column) for column in columns)
    old_column_values = ", ".join("old." + escape_sqlite(column) for column in columns)
    # mask is a bit mask of all columns, so len(columns)
    mask = 2 ** len(columns) - 1
    insert_trigger = """
CREATE TRIGGER {table}_insert_history
AFTER INSERT ON {table}
BEGIN
    INSERT INTO _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
    VALUES (new.rowid, {new_column_values}, 1, cast((julianday('now') - 2440587.5) * 86400.0 * 1000 as integer), {mask});
END;
""".format(
        table=table,
        column_names=column_names,
        new_column_values=new_column_values,
        mask=mask,
    )
    update_columns = []
    for column in columns:
        update_columns.append(
            """
        CASE WHEN old.{column} != new.{column} then new.{column} else null end""".format(
                column=escape_sqlite(column)
            )
        )
    update_columns_sql = ", ".join(update_columns)
    mask_sql = " + ".join(
        """(CASE WHEN old.{column} != new.{column} then {base} else 0 end)""".format(
            column=escape_sqlite(column),
            base=2**idx,
        )
        for idx, column in enumerate(columns)
    )
    where_sql = " or ".join(
        "old.{column} != new.{column}".format(column=escape_sqlite(column))
        for column in columns
    )
    update_trigger = """
CREATE TRIGGER {table}_update_history
AFTER UPDATE ON {table}
FOR EACH ROW
BEGIN
    INSERT INTO _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
    SELECT old.rowid, {update_columns_sql},
        (SELECT MAX(_version) FROM _{table}_history WHERE _rowid = old.rowid) + 1,
        cast((julianday('now') - 2440587.5) * 86400.0 * 1000 as integer),
        {mask_sql}
    WHERE {where_sql};
END;
""".format(
        table=table,
        column_names=column_names,
        update_columns_sql=update_columns_sql,
        mask_sql=mask_sql,
        where_sql=where_sql,
    )
    delete_trigger = """
CREATE TRIGGER {table}_delete_history
AFTER DELETE ON {table}
BEGIN
    INSERT INTO _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
    VALUES (
        old.rowid,
        {old_column_values},
        (SELECT COALESCE(MAX(_version), 0) from _{table}_history WHERE _rowid = old.rowid) + 1,
        cast((julianday('now') - 2440587.5) * 86400.0 * 1000 as integer),
        -1
    );
END;
""".format(
        table=table, column_names=column_names, old_column_values=old_column_values
    )
    return insert_trigger + update_trigger + delete_trigger


def backfill_sql(table, columns):
    column_names = ", ".join(escape_sqlite(column) for column in columns)
    sql = """
INSERT INTO _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
SELECT rowid, {column_names}, 1, cast((julianday('now') - 2440587.5) * 86400.0 * 1000 as integer), {mask}
FROM {table};
""".format(
        table=table, column_names=column_names, mask=2 ** len(columns) - 1
    )
    return sql


def table_columns_and_types(db, table):
    cursor = db.execute(f"PRAGMA table_info([{table}]);")
    columns_and_types = []
    for row in cursor.fetchall():
        columns_and_types.append((row[1], row[2]))
    return columns_and_types


# From https://www.sqlite.org/lang_keywords.html
reserved_words = set(
    (
        "abort action add after all alter analyze and as asc attach autoincrement "
        "before begin between by cascade case cast check collate column commit "
        "conflict constraint create cross current_date current_time "
        "current_timestamp database default deferrable deferred delete desc detach "
        "distinct drop each else end escape except exclusive exists explain fail "
        "for foreign from full glob group having if ignore immediate in index "
        "indexed initially inner insert instead intersect into is isnull join key "
        "left like limit match natural no not notnull null of offset on or order "
        "outer plan pragma primary query raise recursive references regexp reindex "
        "release rename replace restrict right rollback row savepoint select set "
        "table temp temporary then to transaction trigger union unique update using "
        "vacuum values view virtual when where with without"
    ).split()
)

_boring_keyword_re = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def escape_sqlite(s):
    if _boring_keyword_re.match(s) and (s.lower() not in reserved_words):
        return s
    else:
        return f"[{s}]"
