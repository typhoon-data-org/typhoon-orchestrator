from typhoon.contrib.hooks.dbapi_hooks import BigQueryHook


def load_csv(hook: BigQueryHook, table: str, path: str):
    hook.load_csv(table, path)
