from io import BytesIO


def transform_response(response):
    from core import Obj

    if isinstance(response, dict):
        for k, v in response.items():
            response[k] = transform_response(v)
    elif isinstance(response, list):
        for i, x in enumerate(response):
            response[i] = transform_response(x)
    elif isinstance(response, bytes):
        response = f"b'{response.decode('utf-8')}'"
    elif isinstance(response, BytesIO):
        response = f"BytesIO(b'{response.getvalue().decode('utf-8')}')"
    elif isinstance(response, Obj):
        attrs = ', '.join([f'{k}={v}' for k, v in response.__dict__.items()])
        response = f'Obj({attrs})'
    return response
