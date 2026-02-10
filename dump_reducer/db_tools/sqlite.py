import sqlite3
from typing import Any, Dict, List
from ..utils import ensure_readonly_sql, qi
from .base import BaseDbTools

class SqliteTools(BaseDbTools):
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._persistent_conn = None

    def _connect(self):
        if self._persistent_conn:
            return self._persistent_conn
        return sqlite3.connect(self.db_path)

    def set_persistent(self, val: bool):
        if val:
            if not self._persistent_conn:
                self._persistent_conn = sqlite3.connect(self.db_path)
        else:
            if self._persistent_conn:
                self._persistent_conn.close()
                self._persistent_conn = None

    def get_schema(self) -> Dict[str, Any]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = cur.fetchall()
            
            assembled: List[Dict[str, Any]] = []
            for t_row in tables:
                tname = t_row["name"]
                
                cur.execute(f"PRAGMA table_info({qi(tname)})")
                cols_rows = cur.fetchall()
                cols = []
                pks = []
                for c in cols_rows:
                    cols.append({"name": c["name"], "type": c["type"]})
                    if c["pk"] > 0:
                        pks.append(c["name"])
                
                cur.execute(f"PRAGMA foreign_key_list({qi(tname)})")
                fks_rows = cur.fetchall()
                fks = []
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
            lines = []
            for line in conn.iterdump():
                if line.startswith("INSERT INTO"):
                    continue
                
                if tables:
                    should_include = False
                    for t in tables:
                        if f"CREATE TABLE {qi(t)}" in line or f"CREATE TABLE {t}" in line:
                            should_include = True
                            break
                        if t in line:
                            should_include = True
                            break
                    
                    if not should_include:
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

    def execute_sql(self, sql: str) -> None:
        with self._connect() as conn:
            conn.execute(sql)
            conn.commit()

    def setup_subset_schema(self, subset_schema: str, tables: List[str] = None) -> None:
        
        self.set_persistent(True)
        
        with self._connect() as conn:
            conn.execute(f"ATTACH DATABASE ':memory:' AS {qi(subset_schema)}")
            
            schema_info = self.get_schema()
            source_tables = {t["name"] for t in schema_info["tables"]}
            
            target_tables = tables if tables else list(source_tables)
            for t in target_tables:
                if t not in source_tables:
                    continue
                cur = conn.cursor()
                cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (t,))
                row = cur.fetchone()
                if row:
                    create_sql = row[0]
                    if f"CREATE TABLE {qi(t)}" in create_sql:
                        new_sql = create_sql.replace(f"CREATE TABLE {qi(t)}", f"CREATE TABLE {qi(subset_schema)}.{qi(t)}")
                    elif f"CREATE TABLE {t}" in create_sql:
                        new_sql = create_sql.replace(f"CREATE TABLE {t}", f"CREATE TABLE {qi(subset_schema)}.{qi(t)}")
                    else:
                        new_sql = create_sql.replace("CREATE TABLE", f"CREATE TABLE {qi(subset_schema)}.{qi(t)}", 1)
                    
                    conn.execute(new_sql)
            conn.commit()

    def dump_schema_data(self, schema: str, output_path: str, tables: List[str] = None) -> None:
        """
        Exports data as INSERT statements.
        """
        with self._connect() as conn:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("PRAGMA foreign_keys = OFF;\n")
                f.write("BEGIN TRANSACTION;\n")
                
                cur = conn.cursor()
                
                if schema == 'main':
                    table_query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                    table_args = []
                else:
                    table_query = f"SELECT name FROM {qi(schema)}.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                    table_args = []
                
                cur.execute(table_query)
                all_tables = [r[0] for r in cur.fetchall()]
                
                if tables:
                    target_tables = [t for t in tables if t in all_tables]
                else:
                    target_tables = all_tables
                
                for tname in target_tables:
                    qualified_table = f"{qi(schema)}.{qi(tname)}" if schema != 'main' else qi(tname)
                    
                    cur.execute(f"PRAGMA {f'{qi(schema)}.' if schema != 'main' else ''}table_info({qi(tname)})")
                    cols = [r[1] for r in cur.fetchall()]
                    col_list = ", ".join([qi(c) for c in cols])
                    
                    cur.execute(f"SELECT * FROM {qualified_table}")
                    for row in cur:
                        values = []
                        for val in row:
                            if val is None:
                                values.append("NULL")
                            elif isinstance(val, (int, float)):
                                values.append(str(val))
                            else:
                                escaped = str(val).replace("'", "''")
                                values.append(f"'{escaped}'")
                        
                        val_list = ", ".join(values)
                        f.write(f"INSERT INTO {qualified_table} ({col_list}) VALUES ({val_list});\n")
                    
                f.write("COMMIT;\n")
                f.write("PRAGMA foreign_keys = ON;\n")