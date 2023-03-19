from abc import ABC
from pydantic import BaseModel, Field, HttpUrl, FileUrl
import duckdb
import pathlib
from typing import Callable, Any

from .provider import Provider, Extension


class Endpoint(BaseModel, ABC):
    provider: Provider = Field(...)
    uri: HttpUrl | FileUrl = Field(...)
    extension: Extension | None

    def uri_stem(self) -> str | None:
        if self.uri.path is not None:
            return pathlib.Path(self.uri.path).stem
        else:
            return None

    def uri_ext(self) -> str | None:
        if self.uri.path is not None:
            return pathlib.Path(self.uri.path).suffix[1:]
        else:
            return None


class Extract(Endpoint):
    register_name: str | None = Field(default=None, alias='register')

    def local_exec(self) -> duckdb.DuckDBPyRelation:
        input_func: Callable[..., duckdb.DuckDBPyRelation]
        match self.extension:
            case Extension.CSV:
                input_func = duckdb.read_csv
            case Extension.JSON:
                input_func = duckdb.read_json
            case Extension.PARQUET:
                input_func = duckdb.read_parquet
            case None:
                ext = self.uri_ext()
                if ext is not None and ext in [e.value for e in Extension]:
                    self.extension = Extension[ext.upper()]
                    return self.local_exec()
                else:
                    raise ValueError

        try:
            if self.uri.path is not None:
                return input_func(self.uri.path)
            else:
                return input_func(self.uri)
        except:
            raise ValueError

    def http_exec(self) -> duckdb.DuckDBPyRelation:
        return self.local_exec()

    def s3_exec(self) -> duckdb.DuckDBPyRelation:
        return self.local_exec()


class Load(Endpoint):

    def local_exec(self, rel: duckdb.DuckDBPyRelation) -> None:
        input_func: Callable[..., None]
        options: dict[str, Any] = {}
        match self.extension:
            case Extension.CSV:
                input_func = rel.write_csv
                options['sep'] = ','
                options['header'] = True
            case Extension.JSON:
                raise NotImplementedError
            case Extension.PARQUET:
                input_func = rel.write_parquet
                options['compression'] = 'snappy'
            case None:
                ext = self.uri_ext()
                if ext is not None and ext in [e.value for e in Extension]:
                    self.extension = Extension[ext.upper()]
                    return self.local_exec(rel)
                else:
                    raise ValueError

        try:
            if self.uri.path is not None:
                return input_func(self.uri.path, **options)
            else:
                return input_func(self.uri, **options)
        except:
            raise ValueError

    def http_exec(self) -> duckdb.DuckDBPyRelation:
        raise NotImplementedError

    def s3_exec(self) -> duckdb.DuckDBPyRelation:
        raise NotImplementedError
