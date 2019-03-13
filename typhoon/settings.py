import os
import sys


def typhoon_directory():
    try:
        return os.environ['TYPHOON_HOME']
    except KeyError:
        print('Error getting typhoon home directory. Please define TYPHOON_HOME environment variable.')
        sys.exit(-1)


def dags_directory():
    return os.path.join(typhoon_directory(), 'dags')


def out_directory():
    return os.path.join(typhoon_directory(), 'out')


def functions_directory():
    return os.path.join(typhoon_directory(), 'functions')


def adapters_directory():
    return os.path.join(typhoon_directory(), 'adapters')


def transformations_directory():
    return os.path.join(typhoon_directory(), 'transformations')


def hooks_directory():
    return os.path.join(typhoon_directory(), 'hooks')


def get_env():
    return os.environ["TYPHOON-ENV"]
