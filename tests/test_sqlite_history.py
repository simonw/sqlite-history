import sqlite_utils
from sqlite_history.sql import history_table_sql, triggers_sql
from unittest.mock import ANY


def test_create_history_table():
    db = sqlite_utils.Database(memory=True)
    db["test"].create({"id": int, "name": str}, pk="id")
    db.executescript(history_table_sql("test", ["id integer", "name text"]))
    assert db["_test_history"].schema == (
        "CREATE TABLE _test_history (\n"
        "    _rowid INTEGER,\n"
        "   id integer,\n"
        "   name text,\n"
        "    _version INTEGER,\n"
        "    _updated INTEGER,\n"
        "    _mask INTEGER\n"
        ")"
    )


def test_triggers():
    db = sqlite_utils.Database(memory=True)
    db["test"].create({"id": int, "name": str}, pk="id")
    db.executescript(history_table_sql("test", ["id integer", "name text"]))
    db.executescript(triggers_sql("test", ["id", "name"]))
    db["test"].insert({"id": 1, "name": "Alice"})
    db["test"].insert({"id": 2, "name": "Bob"})
    db["test"].update(1, {"name": "Alice Smith"})
    assert list(db["test"].rows) == [
        {"id": 1, "name": "Alice Smith"},
        {"id": 2, "name": "Bob"},
    ]
    assert list(db["_test_history"].rows) == [
        {
            "_rowid": 1,
            "id": 1,
            "name": "Alice",
            "_version": 1,
            "_updated": ANY,
            "_mask": 3,
        },
        {
            "_rowid": 2,
            "id": 2,
            "name": "Bob",
            "_version": 1,
            "_updated": ANY,
            "_mask": 3,
        },
        {
            "_rowid": 1,
            "id": None,
            "name": "Alice Smith",
            "_version": 2,
            "_updated": ANY,
            "_mask": 2,
        },
    ]


def test_triggers_delete():
    db = sqlite_utils.Database(memory=True)
    db["test"].create({"id": int, "name": str}, pk="id")
    db.executescript(history_table_sql("test", ["id integer", "name text"]))
    db.executescript(triggers_sql("test", ["id", "name"]))
    db["test"].insert({"id": 1, "name": "Alice"})
    db["test"].insert({"id": 2, "name": "Bob"})
    db["test"].update(1, {"name": "Alice Smith"})
    db["test"].delete(1)
    assert list(db["test"].rows) == [
        {"id": 2, "name": "Bob"},
    ]
    assert list(db["_test_history"].rows) == [
        {
            "_rowid": 1,
            "id": 1,
            "name": "Alice",
            "_version": 1,
            "_updated": ANY,
            "_mask": 3,
        },
        {
            "_rowid": 2,
            "id": 2,
            "name": "Bob",
            "_version": 1,
            "_updated": ANY,
            "_mask": 3,
        },
        {
            "_rowid": 1,
            "id": None,
            "name": "Alice Smith",
            "_version": 2,
            "_updated": ANY,
            "_mask": 2,
        },
        {
            "_rowid": 1,
            "id": 1,
            "name": "Alice Smith",
            "_version": 3,
            "_updated": ANY,
            "_mask": -1,
        },
    ]
