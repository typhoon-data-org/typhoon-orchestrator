import os
import sys


def typhoon_home():
    try:
        return os.environ['TYPHOON_HOME']
    except KeyError:
        print('Error getting typhoon home directory. Please define TYPHOON_HOME environment variable.')
        print(f'To define in current directory run: export TYPHOON_HOME=$(pwd)')
        sys.exit(-1)


def dags_directory():
    return os.path.join(typhoon_home(), 'dags')


def out_directory():
    return os.path.join(typhoon_home(), 'out')


def functions_directory():
    return os.path.join(typhoon_home(), 'functions')


def adapters_directory():
    return os.path.join(typhoon_home(), 'adapters')


def transformations_directory():
    return os.path.join(typhoon_home(), 'transformations')


def hooks_directory():
    return os.path.join(typhoon_home(), 'hooks')


def environment():
    return os.environ["TYPHOON_ENV"]
