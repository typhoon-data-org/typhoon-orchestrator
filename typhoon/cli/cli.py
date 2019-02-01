import argparse
import os

from zappa.cli import ZappaCLI

from deployment.deploy import copy_user_defined_code, build_zappa_settings
from deployment.settings import out_directory
from typhoon.deployment.dags import load_dags
from typhoon.deployment.deploy import build_dag_code, clean_out


# noinspection PyUnusedLocal
def build_dags(args):
    clean()

    dags = load_dags()
    for dag in dags:
        build_dag_code(dag)

    build_zappa_settings(dags, args.profile, args.project_name, args.s3_bucket)

    copy_user_defined_code()


def clean(args=None):
    clean_out()


def deploy(args=None):
    os.chdir(out_directory())
    zappa_cli = ZappaCLI()
    zappa_cli.handle(['deploy', args.target])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Typhoon CLI')
    subparsers = parser.add_subparsers(help='sub-command help')

    build_dags_parser = subparsers.add_parser('build-dags', help='Build code for dags in $TYPHOON_HOME/out/')
    build_dags_parser.add_argument('--profile', type=str, help='AWS profile used to deploy')
    build_dags_parser.add_argument('--project-name', type=str)
    build_dags_parser.add_argument('--s3-bucket', type=str, required=False)
    build_dags_parser.set_defaults(func=build_dags)

    clean_parser = subparsers.add_parser('clean', help='Clean $TYPHOON_HOME/out/')
    clean_parser.set_defaults(func=clean)

    deploy_parser = subparsers.add_parser('deploy', help='Deploy Typhoon scheduler')
    build_dags_parser.add_argument('--target', type=str, help='Target environment')
    deploy_parser.set_defaults(func=deploy)

    _args = parser.parse_args()
    _args.func(_args)
