import json
import os
from typing import Optional

from botocore.exceptions import ClientError

from typhoon.aws.exceptions import TyphoonResourceCreationError, TyphoonResourceDeletionError
from typhoon.aws.plumbing.boto3_plumbing import boto3_session
from typhoon.core import get_typhoon_config


def _render_assume_policy():
    with open(os.path.join(os.path.dirname(__file__), 'templates', 'iam', 'iam_assume_policy.json')) as f:
        return f.read()


def _render_attach_policy():
    with open(os.path.join(os.path.dirname(__file__), 'templates', 'iam', 'iam_attach_policy.json')) as f:
        return f.read()


def _role_exists(role_name, session) -> bool:
    return role_name in (x['RoleName'] for x in session.client('iam').list_roles()['Roles'])


def create_role(role_name: str, assume_policy: str, aws_profile: Optional[str] = None):
    print(f'Creating {role_name} IAM Role...')
    session = boto3_session(aws_profile)
    iam = session.resource('iam')

    if _role_exists(role_name, session):
        raise TyphoonResourceCreationError(f'Role {role_name} already exists')

    role = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_policy,
    )

    return role


def create_attach_policy(role_name: str, attach_policy: str, aws_profile: Optional[str] = None):
    session = boto3_session(aws_profile)
    iam = session.resource('iam')

    policy = iam.RolePolicy(role_name, 'typhoon-permissions')
    try:
        if policy.policy_document != json.loads(attach_policy):
            print("Updating typhoon-permissions policy on " + role_name + " IAM Role.")

            policy.put(PolicyDocument=attach_policy)
        else:
            print(f'Skipping typhoon-permissions policy creation...')
            # updated = True

    except ClientError:
        print("Creating typhoon-permissions policy on " + role_name + " IAM Role...")
        policy.put(PolicyDocument=attach_policy)
        # updated = True

    return policy


def deploy_role(use_cli_config: bool = False, target_env: Optional[str] = None):
    """Create a role that has sufficient permissions to run DAGs"""
    config = get_typhoon_config(use_cli_config, target_env)
    role_name = config.iam_role_name

    role = None
    try:
        print(f'Creating role {role_name}')
        role = create_role(role_name, _render_assume_policy(), config.aws_profile)
    except TyphoonResourceCreationError:
        print(f'Role {role_name} already exists. Skipping creation...')

    create_attach_policy(role_name, _render_attach_policy(), config.aws_profile)

    return role


def delete_role(role_name, aws_profile: Optional[str] = None):
    print(f'Deleting {role_name} IAM Role...')
    session = boto3_session(aws_profile)
    iam = session.resource('iam')
    if _role_exists(role_name, session):
        role = iam.Role(role_name)
        role.delete()
    else:
        raise TyphoonResourceDeletionError(f'Role {role_name} already exists')


def clean_role(use_cli_config: bool = False, target_env: Optional[str] = None):
    """Delete the given role"""
    config = get_typhoon_config(use_cli_config, target_env)
    role_name = config.iam_role_name

    try:
        delete_role(role_name, config.aws_profile)
    except TyphoonResourceDeletionError:
        print(f'Role {role_name} does not exist. Skipping deletion...')
