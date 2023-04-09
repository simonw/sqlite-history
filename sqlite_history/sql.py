import re


def history_table_sql(table, columns_and_types):
    """Return SQL for history table for a table and its columns."""
    if isinstance(columns_and_types, dict):
        columns_and_types = dict.items()
    # column_names = ",\n".join("   " + column for column in columns_and_types)
    column_names = ",\n".join(
        "   {name} {type}".format(name=escape_sqlite(name), type=type)
        for name, type in columns_and_types
    )
    return """
create table _{table}_history (
    _rowid integer,
{column_names},
    _version integer,
    _updated integer,
    _mask integer
);
create index idx_{table}_history_rowid on _{table}_history (_rowid);
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
create trigger {table}_insert_history
after insert on {table}
begin
    insert into _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
    values (new.rowid, {new_column_values}, 1, strftime('%s', 'now'), {mask});
end;
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
        case when old.{column} != new.{column} then new.{column} else null end""".format(
                column=column
            )
        )
    update_columns_sql = ", ".join(update_columns)
    mask_sql = " + ".join(
        """(case when old.{column} != new.{column} then {base} else 0 end)""".format(
            column=column,
            base=2**idx,
        )
        for idx, column in enumerate(columns)
    )
    where_sql = " or ".join(
        "old.{column} != new.{column}".format(column=column) for column in columns
    )
    update_trigger = """
create trigger {table}_update_history
after update on {table}
for each row
begin
    insert into _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
    select old.rowid, {update_columns_sql},
        (select max(_version) from _{table}_history where _rowid = old.rowid) + 1,
        strftime('%s', 'now'),
        {mask_sql}
    where {where_sql};
end;
""".format(
        table=table,
        column_names=column_names,
        update_columns_sql=update_columns_sql,
        mask_sql=mask_sql,
        where_sql=where_sql,
    )
    delete_trigger = """
create trigger {table}_delete_history
after delete on {table}
begin
    insert into _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
    values (
        old.rowid,
        {old_column_values},
        (select coalesce(max(_version), 0) from _{table}_history where _rowid = old.rowid) + 1,
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
insert into _{table}_history (_rowid, {column_names}, _version, _updated, _mask)
select rowid, {column_names}, 1, strftime('%s', 'now'), {mask}
from {table};
""".format(
        table=table, column_names=column_names, mask=2 ** len(columns) - 1
    )
    return sql


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
