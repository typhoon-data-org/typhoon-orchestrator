from typing import List

from typhoon.remotes import Remotes


def get_remote_names(ctx, args, incomplete) -> List[str]:
    return [x for x in Remotes.remote_names if incomplete in x]