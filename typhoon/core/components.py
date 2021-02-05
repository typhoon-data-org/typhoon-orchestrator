from pydantic import BaseModel, Field
from typhoon.core.dags import IDENTIFIER_REGEX, NodeDefinitionV2
from typing import Dict

from yaml import ScalarNode

ScalarNode.


class Component(BaseModel):
    name: str = Field(..., regex=IDENTIFIER_REGEX, description='Name of your DAG')
    input: Dict[str, ]
    tasks: Dict[str, NodeDefinitionV2]
