"""Example module with data transformations"""


def data_lake_path(date_string: str, system: str, part: int, extension: str) -> str:
    return f'/data/{system}/{date_string}/part{part}.{extension}'
