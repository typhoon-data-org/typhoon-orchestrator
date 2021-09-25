import re
import sys
from io import BytesIO
from typing import List, Any, Union

from dataclasses import dataclass

from typhoon.core import DagContext
from typhoon.core.settings import Settings


@dataclass
class TransformationResult:
    config_item: str
    result: Any

    @property
    def pretty_result(self) -> Any:
        if isinstance(self.result, BytesIO):
            return f'BytesIO({self.result.getvalue()})'
        elif isinstance(self.result, str):
            return f"'{self.result}'"
        else:
            return str(self.result)


@dataclass
class ErrorTransformationResult:
    config_item: str
    error_type: str
    message: str


def _replace_special_vars(string: str):
    string = re.sub(r'\$(\d+)', r'transformation_results[\1 - 1]', string)
    string = re.sub(r'\$BATCH', 'input_data', string)
    string = re.sub(r'\$DAG_CONTEXT\.([\w]+)', r'dag_context.\1', string)
    string = string.replace('$DAG_CONTEXT', 'dag_context')
    string = string.replace('$BATCH_NUM', '2')
    string = re.sub(r'\$VARIABLE(\.(\w+))', r'TyphoonConfig().metadata_store.get_variable("\g<2>").get_contents()', string)
    return string


def get_user_defined_transformation_modules(transform: str) -> List[str]:
    return re.findall(r'transformations\.(\w+)\.\w+', transform)


def run_transformations(args: dict, input_data: Any, dag_context: DagContext) -> List[Union[TransformationResult, ErrorTransformationResult]]:
    sys.path.append(str(Settings.typhoon_home))

    results = []
    for config_item, raw_transformations in args.items():
        if not isinstance(raw_transformations, list):
            raw_transformations = [raw_transformations]
        transformation_results = []
        has_error = False
        for transform in map(_replace_special_vars, raw_transformations):
            for module in get_user_defined_transformation_modules(transform):
                exec(f'import transformations.{module}')
            try:
                custom_locals = locals()
                result = eval(transform, globals(), custom_locals)
            except Exception as e:
                results.append(
                    ErrorTransformationResult(
                        config_item=config_item,
                        error_type=type(e).__name__,
                        message=str(e)
                    )
                )
                has_error = True
                break
            transformation_results.append(result)

        if not has_error:
            results.append(
                TransformationResult(config_item, transformation_results[-1])
            )

    return results
