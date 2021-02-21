import json
from ast import literal_eval
from enum import Enum
from pathlib import Path

import jinja2
import yaml
from dataclasses import dataclass
from typing import Union


class VariableError(Exception):
    pass


class VariableType(str, Enum):
    STRING = 'string'
    JINJA = 'jinja'
    NUMBER = 'number'
    JSON = 'json'
    YAML = 'yaml'


@dataclass
class Variable:
    id: str
    type: VariableType
    contents: str

    def dict_contents(self):
        dc = self.__dict__.copy()
        dc['type'] = dc['type'].value
        return dc

    def __post_init__(self):
        if isinstance(self.type, str):
            self.type = VariableType(self.type)

    def get_contents(self):
        if self.type is VariableType.STRING:
            return self.contents
        elif self.type is VariableType.JINJA:
            return jinja2.Template(self.contents)
        elif self.type is VariableType.NUMBER:
            try:
                num = literal_eval(self.contents)
                if not isinstance(num, int) and not isinstance(num, float):
                    raise VariableError(f'{num} is not a number')
                return num
            except ValueError:
                raise VariableError(f'{self.contents} is not a number')
        elif self.type is VariableType.JSON:
            return json.loads(self.contents)
        elif self.type is VariableType.YAML:
            return yaml.safe_load(self.contents)
        else:
            assert False

    @staticmethod
    def from_file(path: Union[str, Path]) -> 'Variable':
        split_path = str(Path(path).name).split('.')
        name = split_path[0]
        extension = split_path[-1]
        contents = Path(path).read_text()
        if extension in ['j2', 'jinja', 'jinja2']:
            return Variable(name, VariableType.JINJA, contents)
        elif extension == 'num':
            return Variable(name, VariableType.NUMBER, contents)
        elif extension == 'json':
            return Variable(name, VariableType.JSON, contents)
        elif extension in ['yml', 'yaml']:
            return Variable(name, VariableType.YAML, contents)
        else:
            return Variable(name, VariableType.STRING, contents)
