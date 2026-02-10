from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple
from dataclasses import dataclass

@dataclass
class TableInfo:
    schema: str
    name: str
    columns: List[Tuple[str, str]]
    pk_cols: List[str]
    fks: List[Dict[str, Any]]

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

    @abstractmethod
    def dump_schema_data(self, schema: str, output_path: str, tables: List[str] = None) -> None:
        """
        Exports data from the specified schema to a SQL file using INSERT statements.
        """
        pass

    @abstractmethod
    def execute_sql(self, sql: str) -> None:
        """
        Executes a SQL statement (allowing writes).
        """
        pass

    @abstractmethod
    def setup_subset_schema(self, subset_schema: str, tables: List[str] = None) -> None:
        """
        Creates the subset schema and empty tables (copying structure from source).
        """
        pass
