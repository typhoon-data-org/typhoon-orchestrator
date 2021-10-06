
import json
from typing import Union


# @requires('jmespath')
def search(expression: str, data: Union[str, bytes, dict, list], batching_for_lists: bool = True):
    import jmespath
    if not isinstance(data, (dict, list)):
        data = json.loads(data)
    result = jmespath.search(expression, data)
    if batching_for_lists and isinstance(result, list):
        yield from result
    else:
        yield result
