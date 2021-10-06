
def echo(**kwargs):
    """Can take any keyword argument and prints its name and value"""
    for k, v in kwargs.items():
        print('**', k, '=', v)
    return kwargs
