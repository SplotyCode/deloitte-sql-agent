import sqlite3
from typing import Any, Dict, List
from ..utils import ensure_readonly_sql, qi
from .base import BaseDbTools

class SqliteTools(BaseDbTools):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def get_schema(self) -> Dict[str, Any]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            
            # Get tables
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = cur.fetchall()
            
            assembled: List[Dict[str, Any]] = []
            for t_row in tables:
                tname = t_row["name"]
                
                # columns and PK
                cur.execute(f"PRAGMA table_info({qi(tname)})")
                cols_rows = cur.fetchall()
                cols = []
                pks = []
                for c in cols_rows:
                    cols.append({"name": c["name"], "type": c["type"]})
                    if c["pk"] > 0:
                        pks.append(c["name"])
                
                # foreign keys
                cur.execute(f"PRAGMA foreign_key_list({qi(tname)})")
                fks_rows = cur.fetchall()
                fks = []
                # Group by id (constraint)
                fk_map = {}
                for f in fks_rows:
                    fid = f["id"]
                    if fid not in fk_map:
                        fk_map[fid] = {
                            "constraint": f"fk_{tname}_{fid}",
                            "columns": [],
                            "ref_schema": "main",
                            "ref_table": f["table"],
                            "ref_columns": []
                        }
                    fk_map[fid]["columns"].append(f["from"])
                    fk_map[fid]["ref_columns"].append(f["to"])
                fks = list(fk_map.values())
                
                # row count estimate (exact for SQLite usually)
                cur.execute(f"SELECT count(*) as count FROM {qi(tname)}")
                count = cur.fetchone()["count"]
                
                assembled.append({
                    "schema": "main",
                    "name": tname,
                    "columns": cols,
                    "primary_key": pks,
                    "foreign_keys": fks,
                    "row_estimate": count
                })
        return {"tables": assembled}

    def get_stats(self, table: str) -> Dict[str, Any]:
        # SQLite doesn't have pg_stats. We can do a quick sampling or just return empty for now.
        # Or we can compute some basic stats if needed.
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({qi(table)})")
            cols = cur.fetchall()
            
            out = []
            cur.execute(f"SELECT count(*) as total FROM {qi(table)}")
            total = cur.fetchone()["total"]
            
            for c in cols:
                col_name = c["name"]
                if total > 0:
                    cur.execute(f"SELECT count(*) as nulls FROM {qi(table)} WHERE {qi(col_name)} IS NULL")
                    nulls = cur.fetchone()["nulls"]
                    cur.execute(f"SELECT count(DISTINCT {qi(col_name)}) as distincts FROM {qi(table)}")
                    distincts = cur.fetchone()["distincts"]
                else:
                    nulls = 0
                    distincts = 0
                
                out.append({
                    "column": col_name,
                    "null_frac": nulls / total if total > 0 else 0,
                    "n_distinct": distincts,
                    "most_common_vals": "N/A in SQLite",
                    "most_common_freqs": "N/A in SQLite"
                })
        return {"table": table, "stats": out}

    def get_ddl(self, tables: List[str] = None) -> str:
        with self._connect() as conn:
            # iterdump returns both schema and data (INSERTs). We only want schema.
            lines = []
            for line in conn.iterdump():
                if line.startswith("INSERT INTO"):
                    continue
                
                # Check if this line creates a table we care about
                if tables:
                    # Very basic check: "CREATE TABLE " + table_name
                    # or "CREATE TABLE " + "table_name"
                    # This is brittle but works for standard SQLite dumps.
                    should_include = False
                    for t in tables:
                        if f"CREATE TABLE {qi(t)}" in line or f"CREATE TABLE {t}" in line:
                            should_include = True
                            break
                        # Also include indexes/triggers related to these tables if possible?
                        # checking strict string match might miss them.
                        # For now, let's include anything that mentions the table name 
                        # OR if it's not a CREATE TABLE statement (comments, etc - risky, might include too much)
                        # Better approach: parse the statement? No, too complex.
                        # Simple approach: Include line if it contains the table name.
                        if t in line:
                            should_include = True
                            break
                    
                    if not should_include:
                         # Keep generic statements? e.g. "BEGIN TRANSACTION"
                         if "TRANSACTION" in line or "COMMIT" in line:
                             pass
                         else:
                             continue
                
                lines.append(line)
            return "\n".join(lines)

    def query_sql(self, sql: str, max_rows: int = 50) -> Dict[str, Any]:
        ensure_readonly_sql(sql)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            if cur.description is None:
                return {"columns": [], "rows": []}
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(max_rows)
        srows = [[None if v is None else str(v) for v in r] for r in rows]
        return {"columns": cols, "rows": srows, "truncated": len(rows) == max_rows}

    def get_ddl(self) -> str:
        with self._connect() as conn:
            # iterdump returns both schema and data (INSERTs). We only want schema.
            lines = []
            for line in conn.iterdump():
                if not line.startswith("INSERT INTO"):
                    lines.append(line)
            return "\n".join(lines)