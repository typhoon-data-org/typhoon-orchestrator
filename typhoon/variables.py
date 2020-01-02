import json
from ast import literal_eval
from enum import Enum

import yaml
from dataclasses import dataclass


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
        if self.type is VariableType.STRING or self.type is VariableType.JINJA:
            return self.contents
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
