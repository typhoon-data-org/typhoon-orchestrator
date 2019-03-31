import os

from setuptools import setup

from typhoon import __version__

with open('README.md') as readme_file:
    long_description = readme_file.read()

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as f:
    required = f.read().splitlines()

# with open(os.path.join(os.path.dirname(__file__), 'test_requirements.txt')) as f:
#     test_required = f.read().splitlines()
test_required = []


postgres = ['psycopg2']
sqlalchemy = ['sqlalchemy']

all_requirements = postgres + sqlalchemy

setup(
    name='typhoon-orchestrator',
    version=__version__,
    packages=['typhoon'],
    install_requires=required,
    extras_require={
        'all': all_requirements,
        'postgres': postgres,
        'sqlalchemy': sqlalchemy,
    },
    tests_require=test_required,
    test_suite='pytest',
    include_package_data=True,
    license='MIT License',
    description='Serverless task orchestrator in AWS cloud',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/biellls/typhoon-orchestrator',
    author='Gabriel Llobera',
    author_email='bielllobera@gmail.com',
    entry_points={
        'console_scripts': [
            'typhoon=typhoon.cli:handle',
        ]
    },
    classifiers=[
        'Environment :: Console',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
    ],
)
