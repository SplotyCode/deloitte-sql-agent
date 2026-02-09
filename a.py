#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg


OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


# ---------------------------
# Helpers: SQL safety + quoting
# ---------------------------

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


# ---------------------------
# OpenRouter client (tool calling)
# ---------------------------

class OpenRouterClient:
    def __init__(self, api_key: str, model: str) -> None:
        self.api_key = api_key
        self.model = model

    def chat(self, messages: List[Dict[str, Any]], tools: List[Dict[str, Any]]) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            # optional but recommended by OpenRouter for attribution:
            "HTTP-Referer": "https://localhost",
            "X-Title": "db-subset-agent",
        }
        payload = {
            "model": self.model,
            "messages": messages,
            "tools": tools,
            "tool_choice": "auto",
            "temperature": 0.2,
        }
        resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=120)
        resp.raise_for_status()
        return resp.json()


# ---------------------------
# Schema model
# ---------------------------

@dataclass
class TableInfo:
    schema: str
    name: str
    columns: List[Tuple[str, str]]           # (col_name, data_type)
    pk_cols: List[str]                       # support 1-col PK best
    fks: List[Dict[str, Any]]                # {constraint, columns, ref_schema, ref_table, ref_columns}


# ---------------------------
# DB tools (read-only)
# ---------------------------

class PgTools:
    def __init__(self, db_url: str, base_schema: str) -> None:
        self.db_url = db_url
        self.base_schema = base_schema

    def _connect(self):
        return psycopg.connect(self.db_url)

    def get_schema(self) -> Dict[str, Any]:
        """
        Returns tables, columns, PKs, FKs, plus row estimates.
        """
        with self._connect() as conn, conn.cursor() as cur:
            # tables
            cur.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type='BASE TABLE'
                  AND table_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY table_schema, table_name
                """
            )
            tables = cur.fetchall()

            # columns
            cur.execute(
                """
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY table_schema, table_name, ordinal_position
                """
            )
            cols_rows = cur.fetchall()

            # primary keys (may be composite)
            cur.execute(
                """
                SELECT tc.table_schema, tc.table_name, kcu.column_name, kcu.ordinal_position
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                 AND tc.table_name = kcu.table_name
                WHERE tc.constraint_type='PRIMARY KEY'
                  AND tc.table_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position
                """
            )
            pk_rows = cur.fetchall()

            # foreign keys (may be composite)
            cur.execute(
                """
                SELECT
                    tc.table_schema,
                    tc.table_name,
                    tc.constraint_name,
                    kcu.column_name,
                    kcu.ordinal_position,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                 AND tc.table_name = kcu.table_name
                JOIN information_schema.constraint_column_usage ccu
                  ON ccu.constraint_name = tc.constraint_name
                 AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type='FOREIGN KEY'
                  AND tc.table_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
                """
            )
            fk_rows = cur.fetchall()

            # row estimates
            cur.execute(
                """
                SELECT n.nspname AS table_schema, c.relname AS table_name, c.reltuples::bigint AS row_estimate
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind='r'
                  AND n.nspname NOT IN ('pg_catalog','information_schema')
                ORDER BY n.nspname, c.relname
                """
            )
            est_rows = cur.fetchall()

        # assemble
        col_map: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        for sch, t, col, dtype in cols_rows:
            col_map.setdefault((sch, t), []).append((col, dtype))

        pk_map: Dict[Tuple[str, str], List[str]] = {}
        for sch, t, col, _pos in pk_rows:
            pk_map.setdefault((sch, t), []).append(col)

        fk_map: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
        # group by constraint (for composites)
        for sch, t, cname, col, _pos, fsch, ft, fcol in fk_rows:
            table_key = (sch, t)
            fk_map.setdefault(table_key, {})
            fk = fk_map[table_key].setdefault(
                cname,
                {"constraint": cname, "columns": [], "ref_schema": fsch, "ref_table": ft, "ref_columns": []},
            )
            fk["columns"].append(col)
            fk["ref_columns"].append(fcol)

        est_map: Dict[Tuple[str, str], int] = {(sch, t): int(est) for sch, t, est in est_rows}

        assembled: List[Dict[str, Any]] = []
        for sch, t in tables:
            assembled.append(
                {
                    "schema": sch,
                    "name": t,
                    "columns": [{"name": c, "type": dt} for c, dt in col_map.get((sch, t), [])],
                    "primary_key": pk_map.get((sch, t), []),
                    "foreign_keys": list(fk_map.get((sch, t), {}).values()),
                    "row_estimate": est_map.get((sch, t), 0),
                }
            )

        return {"tables": assembled}

    def get_pg_stats(self, schema: str, table: str) -> Dict[str, Any]:
        """
        Cheap-ish column stats from pg_stats (requires ANALYZE to be decent).
        """
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT attname, null_frac, n_distinct,
                       most_common_vals::text, most_common_freqs::text
                FROM pg_stats
                WHERE schemaname = %s AND tablename = %s
                ORDER BY attname
                """,
                (schema, table),
            )
            rows = cur.fetchall()
        out = []
        for attname, null_frac, n_distinct, mcv, mcf in rows:
            # keep payload smaller for LLM
            out.append(
                {
                    "column": attname,
                    "null_frac": float(null_frac),
                    "n_distinct": float(n_distinct),
                    "most_common_vals": (mcv[:400] + "…") if mcv and len(mcv) > 400 else mcv,
                    "most_common_freqs": (mcf[:200] + "…") if mcf and len(mcf) > 200 else mcf,
                }
            )
        return {"schema": schema, "table": table, "pg_stats": out}

    def query_sql(self, sql: str, max_rows: int = 50) -> Dict[str, Any]:
        ensure_readonly_sql(sql)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                return {"columns": [], "rows": []}
            cols = [d.name for d in cur.description]
            rows = cur.fetchmany(max_rows)
        # stringify to avoid binary/decimal weirdness
        srows = [[None if v is None else str(v) for v in r] for r in rows]
        return {"columns": cols, "rows": srows, "truncated": len(rows) == max_rows}


# ---------------------------
# SQL generation
# ---------------------------

def build_subset_sql(
    schema_obj: Dict[str, Any],
    plan: Dict[str, Any],
    base_schema: str,
    subset_schema: str = "subset",
) -> str:
    tables = schema_obj["tables"]

    # map table -> pk (single-col only, for now)
    pk1: Dict[Tuple[str, str], Tuple[str, str]] = {}  # (schema, table) -> (pk_col, pk_type)
    coltypes: Dict[Tuple[str, str, str], str] = {}    # (schema, table, col) -> type

    for t in tables:
        sch, name = t["schema"], t["name"]
        for c in t["columns"]:
            coltypes[(sch, name, c["name"])] = c["type"]
        if len(t["primary_key"]) == 1:
            pkcol = t["primary_key"][0]
            pktype = coltypes.get((sch, name, pkcol), "text")
            pk1[(sch, name)] = (pkcol, pktype)

    # anchors from plan
    anchors = plan.get("anchors", [])
    iterations = int(plan.get("iterations", 2))
    max_children_per_fk = int(plan.get("max_children_per_fk", 5))
    table_caps = plan.get("table_caps", {})  # optional dict: table->cap

    lines: List[str] = []
    lines.append("-- Generated by db-subset-agent (OpenRouter tool calling)")
    lines.append("BEGIN;")
    lines.append(f"CREATE SCHEMA IF NOT EXISTS {qi(subset_schema)};")
    lines.append("SET LOCAL statement_timeout = '0';")
    lines.append("SET LOCAL lock_timeout = '0';")
    lines.append("")

    # temp selection tables
    lines.append("-- 1) Temporary selection sets (PKs)")
    for (sch, tname), (pkcol, pktype) in pk1.items():
        if sch != base_schema:
            continue
        lines.append(f"CREATE TEMP TABLE {qi('_sel_' + tname)} ({qi(pkcol)} {pktype} PRIMARY KEY);")
    lines.append("")

    # apply anchors
    lines.append("-- 2) Seed anchors (LLM-selected PK queries)")
    for a in anchors:
        tname = a["table"]
        sel_sql = a["select_pk_sql"].strip()
        if not SQL_READONLY_PREFIX.search(sel_sql) or SQL_FORBIDDEN.search(sel_sql) or ";" in sel_sql.rstrip(";"):
            raise ValueError(f"Anchor select_pk_sql for table={tname} is not a safe single SELECT/WITH/EXPLAIN")
        key = (base_schema, tname)
        if key not in pk1:
            lines.append(f"-- Skipping anchor {tname}: no single-column PK detected.")
            continue
        pkcol, _pktype = pk1[key]
        lines.append(f"INSERT INTO {qi('_sel_' + tname)} ({qi(pkcol)})")
        lines.append(f"{sel_sql}")
        lines.append("ON CONFLICT DO NOTHING;")
        lines.append("")
    lines.append("")

    # build FK list for expansion (single-col FK -> single-col PK only)
    fk_edges: List[Tuple[str, str, str, str]] = []  # child_table, child_pk, fk_col, parent_table
    for t in tables:
        sch, child = t["schema"], t["name"]
        if sch != base_schema:
            continue
        if (sch, child) not in pk1:
            continue
        child_pk, _ = pk1[(sch, child)]
        for fk in t["foreign_keys"]:
            cols = fk["columns"]
            ref_sch = fk["ref_schema"]
            parent = fk["ref_table"]
            ref_cols = fk["ref_columns"]
            if ref_sch != base_schema:
                continue
            if len(cols) == 1 and len(ref_cols) == 1 and (base_schema, parent) in pk1:
                # assume ref col is the PK col (common case); otherwise we still can do upward inclusion by ref col
                fk_edges.append((child, child_pk, cols[0], parent))

    # expansion loops
    lines.append("-- 3) Expand selection sets to keep relations (bounded)")
    for it in range(iterations):
        lines.append(f"-- Iteration {it+1}/{iterations}")

        # upward closure: include parents referenced by selected children
        for child, child_pk, fk_col, parent in fk_edges:
            parent_pk, _ = pk1[(base_schema, parent)]
            cap = table_caps.get(parent)
            cap_clause = f"LIMIT {int(cap)}" if cap else ""
            lines.append(f"-- Upward: {child}.{fk_col} -> {parent}.{parent_pk}")
            lines.append(f"INSERT INTO {qi('_sel_' + parent)} ({qi(parent_pk)})")
            lines.append("SELECT DISTINCT c.{fk} AS {p_pk}".format(
                fk=qi(fk_col),
                p_pk=qi(parent_pk),
            ))
            lines.append(f"FROM {qname(base_schema, child)} c")
            lines.append(f"JOIN {qi('_sel_' + child)} sc ON c.{qi(child_pk)} = sc.{qi(child_pk)}")
            lines.append(f"LEFT JOIN {qi('_sel_' + parent)} sp ON sp.{qi(parent_pk)} = c.{qi(fk_col)}")
            lines.append(f"WHERE c.{qi(fk_col)} IS NOT NULL AND sp.{qi(parent_pk)} IS NULL")
            if cap_clause:
                lines.append(cap_clause)
            lines.append("ON CONFLICT DO NOTHING;")
            lines.append("")

        # downward: include a few children per selected parent (for test coverage)
        for child, child_pk, fk_col, parent in fk_edges:
            child_cap = table_caps.get(child)
            child_cap_clause = f"LIMIT {int(child_cap)}" if child_cap else ""
            parent_pk, _ = pk1[(base_schema, parent)]
            lines.append(f"-- Downward: pick up to {max_children_per_fk} {child} rows per selected {parent}")
            lines.append("WITH candidates AS (")
            lines.append(
                f"  SELECT c.{qi(child_pk)} AS {qi(child_pk)},"
                f"         row_number() OVER (PARTITION BY c.{qi(fk_col)} ORDER BY random()) AS rn"
            )
            lines.append(f"  FROM {qname(base_schema, child)} c")
            lines.append(f"  JOIN {qi('_sel_' + parent)} sp ON c.{qi(fk_col)} = sp.{qi(parent_pk)}")
            lines.append("), picked AS (")
            lines.append(f"  SELECT {qi(child_pk)} FROM candidates WHERE rn <= {max_children_per_fk}")
            lines.append(")")
            lines.append(f"INSERT INTO {qi('_sel_' + child)} ({qi(child_pk)})")
            lines.append(f"SELECT {qi(child_pk)} FROM picked")
            if child_cap_clause:
                lines.append(child_cap_clause)
            lines.append("ON CONFLICT DO NOTHING;")
            lines.append("")

    lines.append("")

    # materialize subset schema tables
    lines.append("-- 4) Materialize subset schema (tables, then data)")
    lines.append(f"SET LOCAL search_path = {qi(subset_schema)}, {qi(base_schema)};")
    lines.append("")
    for (sch, tname), (pkcol, _pktype) in pk1.items():
        if sch != base_schema:
            continue
        # Note: CREATE TABLE ... LIKE doesn't copy foreign keys; CHECK constraints + PK/UNIQUE via INCLUDING INDEXES. :contentReference[oaicite:8]{index=8}
        lines.append(f"DROP TABLE IF EXISTS {qname(subset_schema, tname)} CASCADE;")
        lines.append(
            f"CREATE TABLE {qname(subset_schema, tname)} "
            f"(LIKE {qname(base_schema, tname)} INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING COMMENTS);"
        )
        lines.append(
            f"INSERT INTO {qname(subset_schema, tname)} "
            f"SELECT t.* FROM {qname(base_schema, tname)} t "
            f"JOIN {qi('_sel_' + tname)} s ON t.{qi(pkcol)} = s.{qi(pkcol)};"
        )
        lines.append("")

    lines.append("-- 5) (Optional) Add foreign keys back in subset schema if you want them.")
    lines.append("--     This script does not recreate FKs automatically (keeps it simple + avoids cycles).")
    lines.append("COMMIT;")
    lines.append("")

    return "\n".join(lines)


# ---------------------------
# Agent orchestration
# ---------------------------

TOOLS_SPEC = [
    {
        "type": "function",
        "function": {
            "name": "get_schema",
            "description": "Return database schema: tables, columns, primary keys, foreign keys, row estimates.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pg_stats",
            "description": "Return column stats from pg_stats for a table (cheap summary).",
            "parameters": {
                "type": "object",
                "properties": {
                    "schema": {"type": "string"},
                    "table": {"type": "string"},
                },
                "required": ["schema", "table"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_sql",
            "description": "Run a read-only SQL query (SELECT/WITH/EXPLAIN). Returns at most max_rows rows.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string"},
                    "max_rows": {"type": "integer", "minimum": 1, "maximum": 200},
                },
                "required": ["sql"],
            },
        },
    },
]


SYSTEM_PROMPT = """You are a database subsetting planner.
Goal: Produce a JSON plan that selects a smaller but diverse dataset for testing.

You have tool access:
- get_schema(): tables, columns, PKs, FKs, row estimates.
- get_pg_stats(schema, table): cheap stats from pg_stats (null_frac, n_distinct, MCV).
- query_sql(sql, max_rows): read-only queries only.

Hard rules:
- Do NOT output any actual production data values (no IDs lists, no email addresses, etc.).
- Your final answer must be STRICT JSON only (no markdown, no prose).
- Anchor queries must be read-only and return ONLY the PK column of the anchor table.
- Prefer diversity: choose rows across different enum-like columns, null/non-null, extreme/min/max timestamps/amounts if available, plus a random sample.

Plan JSON format:
{
  "iterations": 2,
  "max_children_per_fk": 5,
  "table_caps": { "orders": 5000, "users": 2000 },
  "anchors": [
     { "table": "users", "select_pk_sql": "SELECT id FROM public.users ORDER BY random() LIMIT 500" }
  ]
}
"""

def run_agent_and_generate(db_url: str, api_key: str, model: str, target_rows: int, out_path: str, base_schema: str):
    tools = PgTools(db_url, base_schema)
    client = OpenRouterClient(api_key, model)

    # A simple heuristic default caps; LLM can override
    schema_obj = tools.get_schema()
    table_count = sum(1 for t in schema_obj["tables"] if t["schema"] == base_schema and len(t.get("primary_key", [])) == 1)
    per_table_cap = max(50, target_rows // max(1, table_count))

    user_prompt = (
        f"Target total rows (rough): {target_rows}. "
        f"Use table_caps to keep the dataset bounded (suggestion: ~{per_table_cap} per table on average). "
        f"Start by calling get_schema() and then pg_stats for a few high-value tables."
    )

    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    tool_impl = {
        "get_schema": lambda **kwargs: tools.get_schema(),
        "get_pg_stats": lambda schema, table, **kwargs: tools.get_pg_stats(schema, table),
        "query_sql": lambda sql, max_rows=50, **kwargs: tools.query_sql(sql, max_rows=max_rows),
    }

    # tool-calling loop
    final_msg: Optional[Dict[str, Any]] = None
    for _step in range(20):
        resp = client.chat(messages, TOOLS_SPEC)
        msg = resp["choices"][0]["message"]
        messages.append(msg)

        tool_calls = msg.get("tool_calls") or []
        if tool_calls:
            for tc in tool_calls:
                name = tc["function"]["name"]
                raw_args = tc["function"].get("arguments") or "{}"
                try:
                    args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except json.JSONDecodeError:
                    args = {}
                try:
                    result = tool_impl[name](**args)
                except Exception as e:
                    result = {"error": f"{type(e).__name__}: {e}"}

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc["id"],
                        "name": name,
                        "content": json.dumps(result),
                    }
                )
            continue

        # no tool calls -> should be final JSON
        final_msg = msg
        break

    if not final_msg or not final_msg.get("content"):
        raise RuntimeError("Agent did not produce a final plan JSON.")

    plan_str = final_msg["content"].strip()
    plan = json.loads(plan_str)

    subset_sql = build_subset_sql(schema_obj=schema_obj, plan=plan, base_schema=base_schema, subset_schema="subset")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(subset_sql)

    print(f"Wrote subset SQL to: {out_path}")


def main():
    ap = argparse.ArgumentParser(description="LLM-assisted DB subsetting planner (PostgreSQL + OpenRouter tools).")
    ap.add_argument("--db-url", required=True, help="PostgreSQL connection string, e.g. postgresql://user:pass@host:5432/db")
    ap.add_argument("--target-rows", type=int, default=50_000, help="Rough total row budget across tables.")
    ap.add_argument("--out", default="subset.sql", help="Output SQL file to create/populate subset schema.")
    ap.add_argument("--schema", default="public", help="Base schema to subset (default: public).")
    ap.add_argument("--model", default="anthropic/claude-3.5-sonnet", help="OpenRouter model id (must support tool calling).")
    ap.add_argument("--api-key", default=os.getenv("OPENROUTER_API_KEY", ""), help="OpenRouter API key (or env OPENROUTER_API_KEY).")
    args = ap.parse_args()

    if not args.api_key:
        print("Missing OpenRouter API key. Set OPENROUTER_API_KEY or pass --api-key.", file=sys.stderr)
        sys.exit(2)

    run_agent_and_generate(
        db_url=args.db_url,
        api_key=args.api_key,
        model=args.model,
        target_rows=args.target_rows,
        out_path=args.out,
        base_schema=args.schema,
    )

if __name__ == "__main__":
    main()
