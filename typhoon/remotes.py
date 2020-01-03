from configparser import ConfigParser
from typing import List, Optional

from typhoon.core.settings import Settings


class _Remotes:
    @property
    def remotes_config(self) -> ConfigParser:
        config = ConfigParser()
        config.read(str(Settings.typhoon_home/'.typhoonremotes'))
        return config

    @property
    def remote_names(self) -> List[str]:
        return list(self.remotes_config.keys())

    def aws_profile(self, remote: Optional[str]) -> str:
        return self.remotes_config[remote]['aws-profile'] if remote else None

    def metadata_db_url(self, remote: str) -> str:
        return self.remotes_config[remote]['metadata-db-url']

    def use_name_as_suffix(self, remote: str) -> bool:
        return self.remotes_config[remote]['use-name-as-suffix']


Remotes = _Remotes()
