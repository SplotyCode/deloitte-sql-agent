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

    def get_ddl(self, tables: List[str] = None) -> str:
        """
        Uses pg_dump to get schema DDL.
        """
        import subprocess
        # db_url can be passed as the connection string argument
        try:
            # -s for schema-only, --no-owner to avoid permission issues on restore
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
        return {"table": table, "stats": out}

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
