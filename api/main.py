import json
from typing import Optional

import uvicorn
import yaml
from fastapi import FastAPI
from pydantic import BaseModel, Field

from typhoon.core.components import Component
from typhoon.core.dags import IDENTIFIER_REGEX, Granularity, DAGDefinitionV2, TaskDefinition, add_yaml_representers
from typhoon.core.glue import load_components, load_component
from typhoon.core.settings import Settings
from typhoon.deployment.packaging import build_all_dags

app = FastAPI()


@app.get("/api/v1/health-check")
def health_check():
    return 'Ok'


@app.get("/api/v1/components")
def get_components():
    typhoon_components = {
        component.name: 'typhoon'
        for component, _ in load_components(kind='typhoon')
    }
    local_components = {
        component.name: 'components'
        for component, _ in load_components(kind='custom')
    }
    return {'components': {**typhoon_components, **local_components}}


@app.get("/api/v1/component/{component_name}")
def get_component(component_name: str) -> Optional[Component]:
    component = load_component(component_name)
    return component


@app.get("/api/v1/component-args-schema/{component_name}")
def get_component_args_schema(component_name: str) -> Optional[Component]:
    pass


@app.get("/api/v1/variables")
def get_variables():
    return {'variables': Settings.metadata_store().get_variables()}


@app.get("/api/v1/connections")
def get_variables():
    return {'connections': Settings.metadata_store().get_connections()}


class BuildArgs(BaseModel):
    name: str = Field(..., regex=IDENTIFIER_REGEX, description='Name of your DAG')
    schedule_interval: str = Field(
        ...,
        regex='(' + '@hourly|@daily|@weekly|@monthly|@yearly|' +
              r'((\*|\?|\d+((\/|\-){0,1}(\d+))*)\s*){5,6}' + '|' +
              r'rate\(\s*1\s+minute\s*\)' + '|' +
              r'rate\(\s*\d+\s+minutes\s*\)' + '|' +
              r'rate\(\s1*\d+\s+hour\s*\)' + '|' +
              r'rate\(\s*\d+\s+hours\s*\)' + '|' +
              ')',
        description='Schedule or frequency on which the DAG should run'
    )
    granularity: Optional[Granularity] = Field(default=None, description='Granularity of DAG')
    base_component: str = Field(..., regex=IDENTIFIER_REGEX, description='Name of base component')
    component_arguments: dict = Field(..., description='Arguments for base component')


@app.post("/api/v1/dag-from-component")
def dag_from_component(build_args: BuildArgs):
    dag = DAGDefinitionV2(
        name=build_args.name,
        schedule_interval=build_args.schedule_interval,
        granularity=build_args.granularity,
        tasks={
            'flow': TaskDefinition(
                component=build_args.base_component,
                args=build_args.component_arguments
            )
        }
    )
    return dag


@app.put("/api/v1/dag")
def create_dag(dag: DAGDefinitionV2):
    dag_file = Settings.dags_directory / f'{dag.name}.yml'
    add_yaml_representers(yaml.SafeDumper)
    dag_yaml = yaml.safe_dump(dag.dict())
    dag_file.write_text(dag_yaml)
    return str(dag_file)


@app.put("/api/v1/dags-build")
def build_dags():
    build_all_dags(remote=None)
    return 'Ok'


def run_api():
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == '__main__':
    run_api()
