
def echo(**kwargs):
    for k, v in kwargs.items():
        print('**', k, '=', v)
    return kwargs
