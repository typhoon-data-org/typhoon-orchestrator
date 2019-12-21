from typing import Optional

import boto3


def boto3_session(profile_name: Optional[str] = None):
    if profile_name:
        return boto3.session.Session(profile_name=profile_name)
    else:
        return boto3
