import pytest

from dump_reducer.db_tools import DialectSpec, PgTools, SqliteTools, create_db_tools
from dump_reducer.db_tools.base import BaseDbTools
from dump_reducer.planner import (
    _build_system_prompt,
    _detect_sql_dialect,
    _extract_plan_json,
    run_agent_and_generate,
)


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


class _FakeTools(BaseDbTools):
    dialect = DialectSpec(
        name="sqlite",
        display_name="SQLite",
        subset_target_description="The target tables live under the attached `subset` database, so write to `subset.table_name`.",
        dedupe_hint="SQLite conflict handling such as `INSERT OR IGNORE`",
    )

    def __init__(self, *, fail_on_execute: bool = False) -> None:
        self.fail_on_execute = fail_on_execute
        self.executed_sql: list[str] = []
        self.setup_calls = 0
        self.cleanup_calls = 0
        self.dump_calls = 0

    @classmethod
    def supports_url(cls, db_url: str) -> bool:
        return True

    @classmethod
    def from_db_url(cls, db_url: str) -> "_FakeTools":
        return cls()

    def get_schema(self):
        return {"tables": []}

    def get_stats(self, table: str):
        return {"table": table, "stats": []}

    def get_ddl(self, tables: list[str] = None) -> str:
        return ""

    def query_sql(self, sql: str, max_rows: int = 50):
        return {"columns": [], "rows": [], "truncated": False}

    def dump_schema_data(self, schema: str, output_path: str, tables=None) -> None:
        self.dump_calls += 1

    def execute_sql(self, sql: str) -> None:
        self.executed_sql.append(sql)
        if self.fail_on_execute:
            raise RuntimeError("UNIQUE constraint failed: user_roles.id")

    def cleanup_dangling_references(self, subset_schema: str) -> None:
        self.cleanup_calls += 1

    def setup_subset_schema(self, subset_schema: str, tables=None) -> None:
        self.setup_calls += 1


class _FakeClient:
    def __init__(self, *args, **kwargs) -> None:
        self.messages_seen: list[list[dict]] = []
        self.responses = [
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": '{"steps":[{"comment":"broken","sql":"INSERT INTO subset.user_roles SELECT * FROM user_roles;"}]}',
                        }
                    }
                ],
                "_cache": {"hit": False},
            },
            {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": '{"steps":[{"comment":"fixed","sql":"INSERT OR IGNORE INTO subset.user_roles SELECT * FROM user_roles;"}]}',
                        }
                    }
                ],
                "_cache": {"hit": False},
            },
        ]

    def chat(self, messages, tools):
        self.messages_seen.append(list(messages))
        return self.responses.pop(0)

    def get_stats(self):
        return {}


def test_run_agent_retries_with_execution_feedback(monkeypatch):
    exploration_tools = _FakeTools()
    first_exec_tools = _FakeTools(fail_on_execute=True)
    second_exec_tools = _FakeTools()
    created_tools = [exploration_tools, first_exec_tools, second_exec_tools]

    def fake_create_db_tools(db_url: str):
        return created_tools.pop(0)

    client = _FakeClient()

    monkeypatch.setattr("dump_reducer.planner.create_db_tools", fake_create_db_tools)
    monkeypatch.setattr("dump_reducer.planner.OpenRouterClient", lambda *args, **kwargs: client)

    result = run_agent_and_generate(
        db_url="sqlite:///tmp/test.db",
        api_key="key",
        model="model",
        target_rows=100,
        out_path="/tmp/out.sql",
        print_openrouter_stats=False,
    )

    assert result["steps"] == 1
    assert result["plan"]["steps"][0]["comment"] == "fixed"
    assert second_exec_tools.executed_sql == ["INSERT OR IGNORE INTO subset.user_roles SELECT * FROM user_roles;"]
    assert second_exec_tools.cleanup_calls == 1
    assert second_exec_tools.dump_calls == 1
    assert any(
        msg.get("role") == "user" and "UNIQUE constraint failed: user_roles.id" in (msg.get("content") or "")
        for msg in client.messages_seen[-1]
    )
