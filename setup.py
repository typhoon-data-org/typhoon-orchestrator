import os

from setuptools import setup, find_packages

from typhoon import __version__

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme_file:
    long_description = readme_file.read()

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as f:
    required = f.read().splitlines()

# with open(os.path.join(os.path.dirname(__file__), 'test_requirements.txt')) as f:
#     test_required = f.read().splitlines()
test_required = ['pytest', 'moto']


postgres = ['psycopg2']
sqlalchemy = ['sqlalchemy']
snowflake = ['snowflake-connector-python']

db_requirements = ['sqlparse']
all_requirements = db_requirements + postgres + sqlalchemy + snowflake
dev_requirements = all_requirements + ['termcolor', 'watchdog', 'tabulate', 'pygments', 'sqlitedict', 'datadiff']

setup(
    name='typhoon-orchestrator',
    version=__version__,
    packages=find_packages(),
    install_requires=required,
    extras_require={
        'all': all_requirements,
        'db': db_requirements,
        'postgres': db_requirements + postgres,
        'sqlalchemy': db_requirements + sqlalchemy,
        'dev': dev_requirements,
        'test': test_required,
    },
    tests_require=test_required,
    test_suite='pytest',
    include_package_data=True,
    license='MIT License',
    description='Serverless task orchestrator in AWS cloud',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/biellls/typhoon-orchestrator',
    author='Gabriel Llobera Salas',
    author_email='bielllobera@gmail.com',
    entry_points={
        'console_scripts': [
            'typhoon=typhoon.cli:cli',
        ]
    },
    classifiers=[
        'Environment :: Console',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
    ],
)
