import os


def typhoon_directory():
    try:
        return os.environ['TYPHOON_HOME']
    except KeyError:
        print('Error getting typhoon home directory. Please define TYPHOON_HOME environment variable.')
        raise


def dags_directory():
    return os.path.join(typhoon_directory(), 'dags')


def out_directory():
    return os.path.join(typhoon_directory(), 'out')


def functions_directory():
    return os.path.join(typhoon_directory(), 'functions')


def transformations_directory():
    return os.path.join(typhoon_directory(), 'transformations')
