from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple
from dataclasses import dataclass

@dataclass
class TableInfo:
    schema: str
    name: str
    columns: List[Tuple[str, str]]           # (col_name, data_type)
    pk_cols: List[str]                       # support 1-col PK best
    fks: List[Dict[str, Any]]                # {constraint, columns, ref_schema, ref_table, ref_columns}

class BaseDbTools(ABC):
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_stats(self, table: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_ddl(self, tables: list[str] = None) -> str:
        pass

    @abstractmethod
    def query_sql(self, sql: str, max_rows: int = 50) -> Dict[str, Any]:
        pass
