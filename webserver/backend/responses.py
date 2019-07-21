from io import BytesIO


def transform_response(response):
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
    return response
