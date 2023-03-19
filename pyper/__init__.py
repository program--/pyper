"""Workflow configuration tool"""

__version__ = "0.1.0"

import yaml

from pydantic import BaseModel, Field
from .endpoint import Extract, Load
from .transform import Transform


class Pyper(BaseModel):
    extract: Extract = Field(...)
    transform: Transform | list[Transform] | None = Field(default=None)
    load: Load = Field(...)

    def exec(self):
        rel = self.extract.local_exec()

        if self.transform is not None:
            if isinstance(self.transform, list):
                # go through each
                pass
            else:
                rel = self.transform.transform_exec(
                    rel,
                    next((n for n in [
                        self.extract.register_name, self.extract.uri_stem()
                    ] if n is not None), 'extract')
                )

        self.load.local_exec(rel)

    class Config:
        title = 'Pyper Workflow'
        smart_union = True


def workflow(path: str) -> Pyper:
    with open(path) as config:
        return Pyper(**yaml.safe_load(config))
