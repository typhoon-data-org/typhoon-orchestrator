from configparser import ConfigParser
from typing import List, Optional

from click import Path

from typhoon.core.settings import Settings


class _Remotes:
    @property
    def remotes_config_path(self) -> Path:
        return Settings.typhoon_home/'.typhoonremotes'

    @property
    def remotes_config(self) -> ConfigParser:
        config = ConfigParser()
        config.read(str(self.remotes_config_path))
        return config

    @property
    def remote_names(self) -> List[str]:
        return [remote for remote in self.remotes_config.keys() if remote != 'DEFAULT']

    def aws_profile(self, remote: Optional[str]) -> str:
        return self.remotes_config[remote]['aws-profile'] if remote else None

    def metadata_db_url(self, remote: str) -> str:
        return self.remotes_config[remote]['metadata-db-url']

    def fernet_key(self, remote: str) -> str:
        return self.remotes_config[remote]['fernet-key']

    def use_name_as_suffix(self, remote: str) -> bool:
        return self.remotes_config[remote].getboolean('use-name-as-suffix')

    def add_remote(
            self,
            remote: str,
            aws_profile: str,
            metadata_db_url: str,
            use_name_as_suffix: bool,
            fernet_key: str = None,
    ):
        config = self.remotes_config
        config[remote] = {}
        config[remote]['aws-profile'] = aws_profile
        config[remote]['metadata-db-url'] = metadata_db_url
        config[remote]['use-name-as-suffix'] = str(use_name_as_suffix).lower()  # HACK: Because configparser can only set strings
        config[remote]['fernet-key'] = airflow_fernet_key
        with open(self.remotes_config_path, 'w') as f:
            config.write(f)

    def remove_remote(self, remote: str):
        config = self.remotes_config
        del config[remote]
        with open(self.remotes_config_path, 'w') as f:
            config.write(f)


Remotes = _Remotes()
