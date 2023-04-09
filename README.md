# sqlite-history

[![PyPI](https://img.shields.io/pypi/v/sqlite-history.svg)](https://pypi.org/project/sqlite-history/)
[![Changelog](https://img.shields.io/github/v/release/simonw/sqlite-history?include_prereleases&label=changelog)](https://github.com/simonw/sqlite-history/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/sqlite-history/blob/main/LICENSE)

Track changes to SQLite tables using triggers

## Installation

Install this library using `pip`:

    pip install sqlite-history

## Usage

This library can be used to configure triggers on a SQLite database such that any inserts, updates or deletes against a table will have their changes recorded in a separate table.

You can enable history tracking for a table using the `enable_history()` function:

    import sqlite_history
    import sqlite3

    conn = sqlite3.connect("data.db")
    conn.execute("CREATE TABLE table1 (id INTEGER PRIMARY KEY, name TEXT)")
    sqlite_history.configure_history(conn, "table1")

Or you can use the CLI interface, available via `python -m sqlite_history`:

    python -m sqlite_history data.db table1 [table2 table3 ...]

Use `--all` to configure it for all tables:

    python -m sqlite_history data.db --all

More details on how this works coming soon.

## Development

To contribute to this library, first checkout the code. Then create a new virtual environment:

    cd sqlite-history
    python -m venv venv
    source venv/bin/activate

Now install the dependencies and test dependencies:

    pip install -e '.[test]'

To run the tests:

    pytest
