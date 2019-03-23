from io import BytesIO


def transform_response(response):
    if isinstance(response, dict):
        for k, v in response.items():
            response[k] = transform_response(v)
    elif isinstance(response, list):
        for i, x in enumerate(response):
            response[i] = transform_response(x)
    elif isinstance(response, bytes):
        response = f"utf8('{response.decode('utf-8')}')"
    elif isinstance(response, BytesIO):
        response = f"utf8BytesIO('{response.getvalue().decode('utf-8')}')"
    return response
