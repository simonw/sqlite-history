def history_table_sql(table, columns_and_types):
    """Return SQL for history table for a table and its columns."""
    column_names = ",\n".join("   " + column for column in columns_and_types)
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
    column_names = ", ".join(columns)
    new_column_values = ", ".join("new." + column for column in columns)
    old_column_values = ", ".join("old." + column for column in columns)
    # mask is a bit mask of all columns, so len(columns)
    mask = 2 ** len(columns) - 1
    insert_trigger = """
CREATE TRIGGER {table}_insert_history
AFTER INSERT ON {table}
BEGIN
    INSERT INTO _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
    VALUES (new.rowid, {new_column_values}, 1, strftime('%s', 'now'), {mask});
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
        CASE WHEN old.{column} != new.{column} THEN new.{column} ELSE NULL END""".format(
                column=column
            )
        )
    update_columns_sql = ", ".join(update_columns)
    mask_sql = " + ".join(
        """(CASE WHEN old.{column} != new.{column} THEN {base} ELSE 0 END)""".format(
            column=column,
            base=2**idx,
        )
        for idx, column in enumerate(columns)
    )
    where_sql = " OR ".join(
        "old.{column} != new.{column}".format(column=column) for column in columns
    )
    update_trigger = """
CREATE TRIGGER {table}_update_history
AFTER UPDATE ON {table}
FOR EACH ROW
BEGIN
    INSERT INTO _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
    SELECT old.rowid, {update_columns_sql},
        (SELECT MAX(_version) FROM _{table}_history WHERE _rowid = old.rowid) + 1,
        strftime('%s', 'now'),
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
        (SELECT COALESCE(MAX(_version), 0) FROM _{table}_history WHERE _rowid = old.rowid) + 1,
        strftime('%s', 'now'),
        -1
    );
END;
""".format(
        table=table, column_names=column_names, old_column_values=old_column_values
    )
    return insert_trigger + update_trigger + delete_trigger


def backfill_sql(table, columns):
    # INSERT INTO _content_history (id, title, body, created, _version, _updated, _mask)
    # SELECT id, title, body, created, 1, strftime('%s', 'now'), 15
    # FROM content;
    column_names = ", ".join(columns)
    sql = """
INSERT INTO _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
SELECT rowid, {column_names}, 1, strftime('%s', 'now'), {mask}
FROM {table};
""".format(
        table=table, column_names=column_names, mask=2 ** len(columns) - 1
    )
    return sql


# Original prototype:

# -- Trigger for INSERT operation
# CREATE TRIGGER content_insert_history
# AFTER INSERT ON content
# BEGIN
#   INSERT INTO _content_history (id, title, body, created, _version, _updated, _mask)
#   VALUES (new.id, new.title, new.body, new.created, 1, strftime('%s', 'now'), 15);
# END;

# -- Trigger for UPDATE operation
# CREATE TRIGGER content_update_history
# AFTER UPDATE ON content
# FOR EACH ROW
# BEGIN
#   INSERT INTO _content_history (id, title, body, created, _version, _updated, _mask)
#   SELECT new.id,
#     CASE WHEN old.title != new.title THEN new.title ELSE NULL END,
#     CASE WHEN old.body != new.body THEN new.body ELSE NULL END,
#     CASE WHEN old.created != new.created THEN new.created ELSE NULL END,
#     (SELECT MAX(_version) FROM _content_history WHERE id = old.id) + 1,
#     strftime('%s', 'now'),
#     (CASE WHEN old.title != new.title THEN 1 ELSE 0 END) +
#     (CASE WHEN old.body != new.body THEN 2 ELSE 0 END) +
#     (CASE WHEN old.created != new.created THEN 4 ELSE 0 END) +
#     (CASE WHEN old.id != new.id THEN 8 ELSE 0 END)
#   WHERE old.title != new.title OR old.body != new.body OR old.created != new.created;
# END;

# -- Trigger for DELETE operation
# CREATE TRIGGER content_delete_history
# AFTER DELETE ON content
# BEGIN
#   INSERT INTO _content_history (id, title, body, created, _version, _updated, _mask)
#   VALUES (
#     old.id,
#     old.title,
#     old.body,
#     old.created,
#     (SELECT COALESCE(MAX(_version), 0) FROM _content_history WHERE id = old.id) + 1,
#     strftime('%s', 'now'),
#     -1
#   );
# END;
