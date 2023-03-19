from enum import Enum
from pydantic import BaseModel, Field
import duckdb


class TransformLanguage(str, Enum):
    PRQL = 'prql'
    SQL = 'sql'


class TransformBackend(str, Enum):
    DUCKDB = 'duckdb'
    POLARS = 'polars'


class Transform(BaseModel):
    lang: TransformLanguage
    backend: TransformBackend
    query: str = Field(...)

    def transform_exec(self, rel: duckdb.DuckDBPyRelation, tbl: str) -> duckdb.DuckDBPyRelation:
        query: str
        match self.lang:
            case TransformLanguage.SQL:
                query = self.query
            case TransformLanguage.PRQL:
                import prql_python as prql
                query = prql.compile(self.query)  # type: ignore

        return rel.query(tbl, query)
