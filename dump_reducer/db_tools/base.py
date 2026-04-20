from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, List, Tuple

@dataclass
class TableInfo:
    schema: str
    name: str
    columns: List[Tuple[str, str]]
    pk_cols: List[str]
    fks: List[Dict[str, Any]]

@dataclass(frozen=True)
class DialectSpec:
    name: str
    display_name: str
    subset_target_description: str
    dedupe_hint: str
    extra_guidance: Tuple[str, ...] = ()

    def to_prompt(self) -> str:
        guidance_lines = [
            "",
            "Dialect:",
            f"- Database engine: {self.display_name}.",
            f"- Use {self.display_name}-compatible SQL only.",
            f"- {self.subset_target_description}",
            "- Multi-statement steps are allowed when needed, for example creating temp tables before inserts.",
            f"- Prefer {self.dedupe_hint} when deduplication is needed.",
        ]
        guidance_lines.extend(f"- {line}" for line in self.extra_guidance)
        return "\n".join(guidance_lines) + "\n"

class BaseDbTools(ABC):
    dialect: ClassVar[DialectSpec]

    @classmethod
    @abstractmethod
    def supports_url(cls, db_url: str) -> bool:
        pass

    @classmethod
    @abstractmethod
    def from_db_url(cls, db_url: str) -> "BaseDbTools":
        pass

    @classmethod
    def dialect_name(cls) -> str:
        return cls.dialect.name

    def build_system_prompt(self, base_prompt: str) -> str:
        return base_prompt + self.dialect.to_prompt()

    @staticmethod
    def _fk_to_compact(
        local_cols: List[str],
        ref_table: str,
        ref_cols: List[str],
        ref_schema: str | None = None,
    ) -> str:
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

    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_stats(self, table: str) -> Dict[str, Any]:
        """
        Response keys are unspecified and platform dependent. They are ment to be used by a llm
        """
        pass

    @abstractmethod
    def get_ddl(self, tables: list[str] = None) -> str:
        pass

    @abstractmethod
    def query_sql(self, sql: str, max_rows: int = 50) -> Dict[str, Any]:
        pass

    @abstractmethod
    def dump_schema_data(self, schema: str, output_path: str, tables: List[str] = None) -> None:
        """
        Exports data from the specified schema to a SQL file using INSERT statements.
        """
        pass

    @abstractmethod
    def execute_sql(self, sql: str) -> None:
        """
        Executes one or more SQL statements (allowing writes).
        """
        pass

    @abstractmethod
    def cleanup_dangling_references(self, subset_schema: str) -> None:
        """
        Removes rows from the subset schema that have foreign key references to non-existent parent rows.
        """
        pass

    @abstractmethod
    def setup_subset_schema(self, subset_schema: str, tables: List[str] = None) -> None:
        """
        Creates the subset schema and empty tables (copying structure from source).
        """
        pass
