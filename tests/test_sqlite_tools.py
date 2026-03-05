import sqlite3
import pytest
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
                    "user_id->users(id)"
                ],
                "row_estimate": 1
            }
        ]
    }

def test_sqlite_get_stats(temp_db):
    tools = SqliteTools(temp_db)
    stats = tools.get_stats("users")
    
    assert stats["table"] == "users"
    assert stats["total_rows"] == 2

    id_stats = next(s for s in stats["stats"] if s["column"] == "id")
    assert id_stats["n_distinct"] == 2
    assert {v[0] for v in id_stats["top_values"]} == {1, 2}
    assert all(v[1] == 1 for v in id_stats["top_values"])
    assert id_stats["null_frac"] == 0.0

    name_stats = next(s for s in stats["stats"] if s["column"] == "name")
    assert name_stats["n_distinct"] == 2
    assert {v[0] for v in name_stats["top_values"]} == {"Alice", "Bob"}
    assert all(v[1] == 1 for v in name_stats["top_values"])
    assert name_stats["null_frac"] == 0.0

    email_stats = next(s for s in stats["stats"] if s["column"] == "email")
    assert email_stats["n_distinct"] == 2
    assert {v[0] for v in email_stats["top_values"]} == {"alice@example.com", "bob@example.com"}
    assert all(v[1] == 1 for v in email_stats["top_values"])
    assert email_stats["null_frac"] == 0.0


def test_sqlite_get_stats_with_qualified_table_name(temp_db):
    tools = SqliteTools(temp_db)
    stats = tools.get_stats('main."users"')

    assert stats["table"] == "users"
    assert stats["total_rows"] == 2

def test_sqlite_get_stats_empty(tmp_path):
    db_path = tmp_path / "test_empty.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE empty_table (id INTEGER PRIMARY KEY, name TEXT)")
    conn.close()
    
    tools = SqliteTools(str(db_path))
    stats = tools.get_stats("empty_table")
    
    assert stats == {
        "table": "empty_table",
        "total_rows": 0,
        "stats": []
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
