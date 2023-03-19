from enum import Enum


class Provider(str, Enum):
    LOCAL = 'local'
    HTTP = 'http'
    S3 = 's3'


class Extension(str, Enum):
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
