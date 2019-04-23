import pytest
from moto import mock_iam

from typhoon.aws import TyphoonResourceCreationError, TyphoonResourceDeletionError
from typhoon.deployment.iam import create_role, delete_role


@mock_iam
def test_create_delete_role():
    create_role(role_name='aaa', policy='{}')
    with pytest.raises(TyphoonResourceCreationError):
        create_role(role_name='aaa', policy='{}')
    delete_role(role_name='aaa')
    with pytest.raises(TyphoonResourceDeletionError):
        delete_role(role_name='aaa')
    create_role(role_name='aaa', policy='{}')
