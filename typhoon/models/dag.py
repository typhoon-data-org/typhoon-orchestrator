import functools


def dag(func):
    # noinspection PyBroadException
    @functools.wraps(func)
    def func_wrapper(event, context):
        try:
            func(event, context)
        except Exception as e:
            print(e)
            # TODO: Mark task as failed in dynamoDB
            return True     # To prevent lambda retries. Errors won't show up in cloudwatch
        finally:
            # TODO: Write log to S3
            pass

    return func_wrapper
