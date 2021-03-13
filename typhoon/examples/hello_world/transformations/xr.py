from typing import List


def flatten_response(response: dict) -> List[dict]:
    result = []
    for k, v in response['rates'].items():
        v = v.copy()
        v['base'] = response['base']
        v['date'] = k
        result.append(v)
    return result
