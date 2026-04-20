import pytest

from dump_reducer.db_tools import PgTools, SqliteTools, create_db_tools
from dump_reducer.planner import _build_system_prompt, _detect_sql_dialect, _extract_plan_json


def test_extract_plan_json_accepts_strict_json():
    plan = _extract_plan_json('{"tables":["users"],"steps":[]}')
    assert plan == {"tables": ["users"], "steps": []}


def test_extract_plan_json_accepts_leading_text_before_fenced_json():
    plan = _extract_plan_json(
        """
        Here is the final plan.

        ```json
        {
          "tables": ["users"],
          "steps": []
        }
        ```
        """
    )
    assert plan == {"tables": ["users"], "steps": []}


def test_extract_plan_json_rejects_missing_json():
    with pytest.raises(Exception):
        _extract_plan_json("No plan here.")


def test_detect_sql_dialect_for_sqlite_and_postgres():
    assert _detect_sql_dialect("/tmp/test.db") == "sqlite"
    assert _detect_sql_dialect("sqlite:///tmp/test.db") == "sqlite"
    assert _detect_sql_dialect("postgresql://localhost/test") == "postgresql"


def test_build_system_prompt_includes_sqlite_dialect_guidance():
    prompt = _build_system_prompt("sqlite")

    assert "Database engine: SQLite." in prompt
    assert "Multi-statement steps are allowed" in prompt
    assert "INSERT OR IGNORE" in prompt


def test_create_db_tools_uses_registered_driver_parsers():
    sqlite_tools = create_db_tools("sqlite:///tmp/test.db")
    postgres_tools = create_db_tools("postgresql://localhost/test")

    assert isinstance(sqlite_tools, SqliteTools)
    assert sqlite_tools.db_path == "/tmp/test.db"
    assert isinstance(postgres_tools, PgTools)
    assert postgres_tools.db_url == "postgresql://localhost/test"
