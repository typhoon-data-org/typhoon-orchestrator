import argparse

from deployment.dags import load_dags
from deployment.deploy import build_dag_code


# noinspection PyUnusedLocal
def build_dags(args=None):
    dags = load_dags()
    for dag in dags:
        build_dag_code(dag)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Typhoon CLI')
    subparsers = parser.add_subparsers(help='sub-command help')

    build_dags_parser = subparsers.add_parser('build-dags', help='Build code for dags in TYPHOON_HOME/out/')
    build_dags_parser.set_defaults(func=build_dags)

    _args = parser.parse_args()
    _args.func(_args)
