from .base import TableInfo, BaseDbTools, DialectSpec
from .postgres import PgTools
from .sqlite import SqliteTools

DB_TOOL_TYPES = (PgTools, SqliteTools)


def get_db_tools_class(db_url: str) -> type[BaseDbTools]:
    for tools_cls in DB_TOOL_TYPES:
        if tools_cls.supports_url(db_url):
            return tools_cls
    raise ValueError(f"Unsupported database URL or path: {db_url}")


def create_db_tools(db_url: str) -> BaseDbTools:
    return get_db_tools_class(db_url).from_db_url(db_url)


def detect_sql_dialect(db_url: str) -> str:
    return get_db_tools_class(db_url).dialect_name()


def get_db_tools_class_for_dialect(dialect: str) -> type[BaseDbTools]:
    normalized = dialect.lower()
    for tools_cls in DB_TOOL_TYPES:
        if tools_cls.dialect_name() == normalized:
            return tools_cls
    raise ValueError(f"Unsupported SQL dialect: {dialect}")


def get_dialect_spec(db_url: str) -> DialectSpec:
    return get_db_tools_class(db_url).dialect


def get_dialect_spec_for_name(dialect: str) -> DialectSpec:
    return get_db_tools_class_for_dialect(dialect).dialect


__all__ = [
    "TableInfo",
    "BaseDbTools",
    "DialectSpec",
    "PgTools",
    "SqliteTools",
    "DB_TOOL_TYPES",
    "get_db_tools_class",
    "create_db_tools",
    "detect_sql_dialect",
    "get_dialect_spec",
    "get_db_tools_class_for_dialect",
    "get_dialect_spec_for_name",
]
