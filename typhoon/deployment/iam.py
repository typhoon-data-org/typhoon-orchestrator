import os
from typing import Optional

from typhoon.aws import boto3_session, TyphoonResourceCreationError, TyphoonResourceDeletionError


def _render_policy():
    with open(os.path.join(os.path.dirname(__file__), 'templates', 'iam', 'iam_policy.json')) as f:
        return f.read()


def _role_exists(role_name, session) -> bool:
    return role_name in (x['RoleName'] for x in session.client('iam').list_roles()['Roles'])


def create_role(role_name: str, policy: str, aws_profile: Optional[str] = None):
    print(f'Creating {role_name} IAM Role...')
    session = boto3_session(aws_profile)
    iam = session.resource('iam')

    if _role_exists(role_name, session):
        raise TyphoonResourceCreationError(f'Role {role_name} already exists')

    role = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=policy,
    )
    return role


def deploy_role(role_name: str, aws_profile: Optional[str] = None):
    """Create a role that has sufficient permissions to run DAGs"""
    role = None
    try:
        role = create_role(role_name, _render_policy(), aws_profile)
    except TyphoonResourceCreationError:
        print(f'Role {role_name} already exists. Skipping creation...')

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


def clean_role(role_name, aws_profile: Optional[str] = None):
    """Delete the given role"""
    try:
        delete_role(role_name, aws_profile)
    except TyphoonResourceDeletionError:
        print(f'Role {role_name} does not exist. Skipping deletion...')
