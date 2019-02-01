import argparse

from deployment.dags import load_dags
from deployment.deploy import build_dag_code, clean_out


# noinspection PyUnusedLocal
def build_dags(args=None):
    dags = load_dags()
    for dag in dags:
        build_dag_code(dag)


def clean(args):
    clean_out()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Typhoon CLI')
    subparsers = parser.add_subparsers(help='sub-command help')

    build_dags_parser = subparsers.add_parser('build-dags', help='Build code for dags in $TYPHOON_HOME/out/')
    build_dags_parser.set_defaults(func=build_dags)

    clean_parser = subparsers.add_parser('clean', help='Clean $TYPHOON_HOME/out/')
    clean_parser.set_defaults(func=clean)

    _args = parser.parse_args()
    _args.func(_args)
