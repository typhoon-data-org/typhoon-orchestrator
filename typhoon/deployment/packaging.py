import os
import subprocess
import sys
import tempfile
from distutils.dir_util import copy_tree
from distutils.errors import DistutilsFileError
from shutil import copytree, copy, make_archive, move, rmtree

from typhoon.core.settings import out_directory, functions_directory, transformations_directory, hooks_directory, \
    typhoon_home


def package_dag(
        dag_name: str,
        venv_path: str,
):
    with tempfile.TemporaryDirectory(prefix=dag_name) as temp_dir:
        _copy_venv_site_packages(temp_dir, venv_path)
        # add_handler(temp_dir)
        try:
            copytree(functions_directory(), os.path.join(temp_dir, 'functions'))
        except FileNotFoundError:
            print('No functions directory')
        try:
            copytree(transformations_directory(), os.path.join(temp_dir, 'transformations'))
        except FileNotFoundError:
            print('No transformations directory')
        try:
            copytree(hooks_directory(), os.path.join(temp_dir, 'hooks'))
        except FileNotFoundError:
            print('No hooks directory')
        try:
            copy(os.path.join(typhoon_home(), 'typhoonconfig.cfg'), temp_dir)
        except FileNotFoundError:
            print('No typhoonconfig.cfg')
        copy(
            src=os.path.join(out_directory(), f'{dag_name}.py'),
            dst=temp_dir,
        )

        make_archive(
            base_name=dag_name,
            format='zip',
            root_dir=temp_dir,
        )
        move(f'{dag_name}.zip', os.path.join(out_directory()))


def _copy_venv_site_packages(temp_dir: str, venv_path: str):
    copy_tree(
        src=_venv_site_packages_path(venv_path),
        dst=temp_dir,
        # symlinks=False,
        # ignore=ignore_patterns('*boto*')
    )
    try:
        copy_tree(
            src=_venv_site_packages_path(venv_path, bin64=True),
            dst=temp_dir,
            # symlinks=False,
            # ignore=ignore_patterns('*boto*')
        )
    except DistutilsFileError:
        pass

    # Deletes boto related packages (included in lambda)
    for name in os.listdir(temp_dir):
        if 'boto' in name:
            rmtree(os.path.join(temp_dir, name))


def _python_version() -> str:
    return 'python{}.{}'.format(*sys.version_info)


def _venv_site_packages_path(venv_path: str, bin64=False) -> str:
    lib = 'lib64' if bin64 else 'lib'
    return os.path.join(venv_path, lib, _python_version(), 'site-packages')


def get_current_venv():
    """
    Returns the path to the current virtualenv
    """
    if 'VIRTUAL_ENV' in os.environ:
        venv = os.environ['VIRTUAL_ENV']
    elif os.path.exists('.python-version'):  # pragma: no cover
        try:
            subprocess.check_output(['pyenv', 'help'], stderr=subprocess.STDOUT)
        except OSError:
            print("This directory seems to have pyenv's local venv, "
                  "but pyenv executable was not found.")
        with open('.python-version', 'r') as f:
            # minor fix in how .python-version is read
            # Related: https://github.com/Miserlou/Zappa/issues/921
            env_name = f.readline().strip()
        bin_path = subprocess.check_output(['pyenv', 'which', 'python']).decode('utf-8')
        venv = bin_path[:bin_path.rfind(env_name)] + env_name
    else:  # pragma: no cover
        return None
    return venv
