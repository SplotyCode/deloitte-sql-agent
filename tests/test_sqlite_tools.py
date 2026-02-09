import sqlite3
import pytest
import os
from dump_reducer.db_tools import SqliteTools

@pytest.fixture
def temp_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
    cursor.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
    cursor.execute("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')")
    
    cursor.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, FOREIGN KEY(user_id) REFERENCES users(id))")
    cursor.execute("INSERT INTO posts (user_id, title) VALUES (1, 'Hello World')")
    
    conn.commit()
    conn.close()
    return str(db_path)

def test_sqlite_get_schema(temp_db):
    tools = SqliteTools(temp_db)
    schema = tools.get_schema()
    
    assert schema == {
        "tables": [
            {
                "schema": "main",
                "name": "users",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "TEXT"},
                    {"name": "email", "type": "TEXT"}
                ],
                "primary_key": ["id"],
                "foreign_keys": [],
                "row_estimate": 2
            },
            {
                "schema": "main",
                "name": "posts",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "user_id", "type": "INTEGER"},
                    {"name": "title", "type": "TEXT"}
                ],
                "primary_key": ["id"],
                "foreign_keys": [
                    {
                        "constraint": "fk_posts_0",
                        "columns": ["user_id"],
                        "ref_schema": "main",
                        "ref_table": "users",
                        "ref_columns": ["id"]
                    }
                ],
                "row_estimate": 1
            }
        ]
    }

def test_sqlite_get_stats(temp_db):
    tools = SqliteTools(temp_db)
    stats = tools.get_stats("users")
    
    assert stats == {
        "table": "users",
        "stats": [
            {
                "column": "id",
                "null_frac": 0.0,
                "n_distinct": 2,
                "most_common_vals": "N/A in SQLite",
                "most_common_freqs": "N/A in SQLite"
            },
            {
                "column": "name",
                "null_frac": 0.0,
                "n_distinct": 2,
                "most_common_vals": "N/A in SQLite",
                "most_common_freqs": "N/A in SQLite"
            },
            {
                "column": "email",
                "null_frac": 0.0,
                "n_distinct": 2,
                "most_common_vals": "N/A in SQLite",
                "most_common_freqs": "N/A in SQLite"
            }
        ]
    }

def test_sqlite_query_sql(temp_db):
    tools = SqliteTools(temp_db)
    res = tools.query_sql("SELECT name FROM users ORDER BY name")
    
    assert res == {
        "columns": ["name"],
        "rows": [["Alice"], ["Bob"]],
        "truncated": False
    }

def test_sqlite_query_sql_forbidden(temp_db):
    tools = SqliteTools(temp_db)
    with pytest.raises(ValueError, match="Only SELECT"):
        tools.query_sql("DELETE FROM users")

