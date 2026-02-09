import re

SQL_READONLY_PREFIX = re.compile(r"^\s*(with\b|select\b|explain\b)", re.IGNORECASE)
SQL_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|comment|do)\b",
    re.IGNORECASE,
)

def qi(ident: str) -> str:
    """Quote identifier for PostgreSQL."""
    return '"' + ident.replace('"', '""') + '"'

def qname(schema: str, name: str) -> str:
    return f"{qi(schema)}.{qi(name)}"

def ensure_readonly_sql(sql: str) -> None:
    if ";" in sql.strip().rstrip(";"):
        # crude but effective: disallow multi-statement and embedded semicolons
        raise ValueError("Only a single SQL statement is allowed (no semicolons).")
    if not SQL_READONLY_PREFIX.search(sql):
        raise ValueError("Only SELECT/WITH/EXPLAIN statements are allowed.")
    if SQL_FORBIDDEN.search(sql):
        raise ValueError("Statement contains forbidden keywords (must be read-only).")
