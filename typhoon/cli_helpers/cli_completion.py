from typing import List

from typhoon.core.settings import Settings
from typhoon.remotes import Remotes


def get_remote_names(ctx, args, incomplete) -> List[str]:
    return [x for x in Remotes.remote_names if incomplete in x]


def get_dag_names(ctx, args, incomplete) -> List[str]:
    return [x.dag_name for x in Settings.metadata_store(aws_profile=None).get_dag_deployments() if incomplete in x.dag_name]
