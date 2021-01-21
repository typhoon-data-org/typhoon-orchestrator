from dataclasses import dataclass

from typhoon.core.templated import Templated


@dataclass
class SelectAll(Templated):
    template = 'SELECT * FROM {{ table_name }}'
    table_name: str
