try:
    import psycopg
except ImportError:
    psycopg = None
from typing import Any, Dict, List, Tuple
from ..utils import ensure_readonly_sql
from .base import BaseDbTools

class PgTools(BaseDbTools):
    def __init__(self, db_url: str) -> None:
        self.db_url = db_url

    def _connect(self):
        if psycopg is None:
            raise ImportError("psycopg is not installed. Run 'pip install psycopg[binary]' to use PostgreSQL.")
        return psycopg.connect(self.db_url)

    @staticmethod
    def _fk_to_compact(local_cols: List[str], ref_schema: str, ref_table: str, ref_cols: List[str]) -> str:
        target = f"{ref_schema}.{ref_table}" if ref_schema else ref_table
        return f"{','.join(local_cols)}->{target}({','.join(ref_cols)})"

    @staticmethod
    def _parse_fk_compact(spec: str) -> tuple[list[str], str, list[str]]:
        lhs, rhs = spec.split("->", 1)
        ref_target, ref_cols_raw = rhs.split("(", 1)
        ref_cols = ref_cols_raw.rstrip(")")
        local_cols = [c.strip() for c in lhs.split(",") if c.strip()]
        ref_table = ref_target.strip().split(".")[-1]
        target_cols = [c.strip() for c in ref_cols.split(",") if c.strip()]
        return local_cols, ref_table, target_cols

    def _fk_components(self, fk: Any) -> tuple[list[str], str, list[str]]:
        if isinstance(fk, str):
            return self._parse_fk_compact(fk)
        return fk["columns"], fk["ref_table"], fk["ref_columns"]

    def get_ddl(self, tables: List[str] = None) -> str:
        """
        Uses pg_dump to get schema DDL.
        """
        import subprocess
        try:
            cmd = ["pg_dump", "-s", "--no-owner", "--no-privileges"]
            if tables:
                for t in tables:
                    cmd.extend(["-t", t])
            cmd.append(self.db_url)
            
            res = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return res.stdout
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            return f"-- pg_dump schema failed or not found: {e}"

    def get_schema(self) -> Dict[str, Any]:
        """
        Returns tables, columns, PKs, FKs, plus row estimates.
        """
        with self._connect() as conn, conn.cursor() as cur:
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

            cur.execute(
                """
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY table_schema, table_name, ordinal_position
                """
            )
            cols_rows = cur.fetchall()

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

        col_map: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        for sch, t, col, dtype in cols_rows:
            col_map.setdefault((sch, t), []).append((col, dtype))

        pk_map: Dict[Tuple[str, str], List[str]] = {}
        for sch, t, col, _pos in pk_rows:
            pk_map.setdefault((sch, t), []).append(col)

        fk_map: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
        for sch, t, cname, col, _pos, fsch, ft, fcol in fk_rows:
            table_key = (sch, t)
            fk_map.setdefault(table_key, {})
            fk = fk_map[table_key].setdefault(
                cname,
                {"columns": [], "ref_schema": fsch, "ref_table": ft, "ref_columns": []},
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
                    "foreign_keys": [
                        self._fk_to_compact(fk["columns"], fk["ref_schema"], fk["ref_table"], fk["ref_columns"])
                        for fk in fk_map.get((sch, t), {}).values()
                    ],
                    "row_estimate": est_map.get((sch, t), 0),
                }
            )

        return {"tables": assembled}

    def get_stats(self, table: str) -> Dict[str, Any]:
        """
        Cheap-ish column stats from pg_stats (requires ANALYZE to be decent).
        """
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT attname, null_frac, n_distinct,
                       most_common_vals::text, most_common_freqs::text
                FROM pg_stats
                WHERE tablename = %s
                ORDER BY attname
                """,
                (table,),
            )
            rows = cur.fetchall()
        out = []
        for attname, null_frac, n_distinct, mcv, mcf in rows:
            out.append(
                {
                    "column": attname,
                    "null_frac": float(null_frac),
                    "n_distinct": float(n_distinct),
                    "most_common_vals": (mcv[:400] + "…") if mcv and len(mcv) > 400 else mcv,
                    "most_common_freqs": (mcf[:200] + "…") if mcf and len(mcf) > 200 else mcf,
                }
            )
        return {"table": table, "stats": out}

    def query_sql(self, sql: str, max_rows: int = 50) -> Dict[str, Any]:
        ensure_readonly_sql(sql)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                return {"columns": [], "rows": []}
            cols = [d.name for d in cur.description]
            rows = cur.fetchmany(max_rows)
        srows = [[None if v is None else str(v) for v in r] for r in rows]
        return {"columns": cols, "rows": srows, "truncated": len(rows) == max_rows}

    def dump_schema_data(self, schema: str, output_path: str, tables: List[str] = None) -> None:
        """
        Uses pg_dump to export data as INSERT statements.
        """
        import subprocess
        cmd = [
            "pg_dump",
            "--data-only",
            "--inserts",
            "--column-inserts",
            "--no-owner",
            "--no-privileges",
            "-n", schema,
            self.db_url
        ]

        if tables:
            for t in tables:
                if "." not in t:
                    cmd.extend(["-t", f"{schema}.{t}"])
                else:
                    cmd.extend(["-t", t])

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                subprocess.run(cmd, stdout=f, check=True, text=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"pg_dump failed: {e}")

    def execute_sql(self, sql: str) -> None:
        """
        Executes SQL (allowing writes/DDL). Automatically commits.
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()

    def cleanup_dangling_references(self, subset_schema: str) -> None:
        schema_info = self.get_schema()
        with self._connect() as conn:
            with conn.cursor() as cur:
                tables = schema_info["tables"]
                for table in tables:
                    tname = table["name"]
                    fks = table["foreign_keys"]
                    for fk in fks:
                        local_cols, ref_table, ref_cols = self._fk_components(fk)
                        q_table = f'"{subset_schema}"."{tname}"'
                        q_ref = f'"{subset_schema}"."{ref_table}"'
                        if len(local_cols) == 1:
                            sql = f"""
                                DELETE FROM {q_table}
                                WHERE "{local_cols[0]}" IS NOT NULL
                                AND "{local_cols[0]}" NOT IN (SELECT "{ref_cols[0]}" FROM {q_ref})
                            """
                            cur.execute(sql)
                        else:
                            join_parts = []
                            for l, r in zip(local_cols, ref_cols):
                                join_parts.append(f'{q_table}."{l}" = {q_ref}."{r}"')
                            join_cond = " AND ".join(join_parts)
                            
                            not_null_cond = " AND ".join([f'{q_table}."{c}" IS NOT NULL' for c in local_cols])
                            
                            sql = f"""
                                DELETE FROM {q_table}
                                WHERE {not_null_cond}
                                AND NOT EXISTS (
                                    SELECT 1 FROM {q_ref}
                                    WHERE {join_cond}
                                )
                            """
                            cur.execute(sql)
            conn.commit()

    def setup_subset_schema(self, subset_schema: str, tables: List[str] = None) -> None:
        """
        Creates the subset schema and copies table structures using LIKE ... INCLUDING ALL.
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {subset_schema};")
                
                schema_info = self.get_schema()
                source_tables_map = {t["name"]: t["schema"] for t in schema_info["tables"]}
                
                target_tables = tables if tables else list(source_tables_map.keys())
                
                for t in target_tables:
                    if t not in source_tables_map:
                        continue
                    src_schema = source_tables_map[t]
                    cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {subset_schema}.{t} "
                        f"(LIKE {src_schema}.{t} INCLUDING ALL);"
                    )
            conn.commit()
