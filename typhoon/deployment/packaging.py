import os
import sys
import tempfile
from shutil import copytree, copy, make_archive, move

from typhoon.settings import out_directory, functions_directory, transformations_directory, hooks_directory, \
    typhoon_directory


def package_dag(
        dag_name: str,
        venv_path: str,
):
    with tempfile.TemporaryDirectory(prefix=dag_name) as temp_dir:
        _copy_venv_site_packages(temp_dir, venv_path)
        # add_handler(temp_dir)
        copytree(functions_directory(), os.path.join(temp_dir, 'functions'))
        copytree(transformations_directory(), os.path.join(temp_dir, 'transformations'))
        copytree(hooks_directory(), os.path.join(temp_dir, 'hooks'))
        copy(os.path.join(typhoon_directory(), 'typhoonconfig.cfg'), temp_dir)
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
    copytree(
        src=_venv_site_packages_path(venv_path),
        dst=temp_dir,
        symlinks=False,
    )
    try:
        copytree(
            src=_venv_site_packages_path(venv_path, bin64=True),
            dst=temp_dir,
            symlinks=False,
        )
    except FileNotFoundError:
        pass


def _python_version() -> str:
    return 'python{}.{}'.format(*sys.version_info)


def _venv_site_packages_path(venv_path: str, bin64=False) -> str:
    lib = 'lib64' if bin64 else 'lib'
    os.path.join(venv_path, lib, _python_version(), 'site-packages')
