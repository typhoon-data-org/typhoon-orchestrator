

# noinspection PyDefaultArgument
def make_test_branch(
        source_function,
        adapter,
        destination_function,
        source_function_config: dict = {},
        adapter_config: dict = {},
        destination_function_config: dict = {},
):
    def call(*args, **kwargs):
        for batch in source_function(*args, **kwargs, **source_function_config):
            data = adapter(data=batch, **adapter_config)
            for result in destination_function(**data, **destination_function_config):
                yield result

    return call
