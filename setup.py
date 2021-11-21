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


kafka = ['kafka-python']

all_requirements = kafka
dev_requirements = all_requirements + [
    'termcolor', 'watchdog', 'tabulate', 'pygments', 'sqlitedict', 'datadiff', 'ipython', 'jedi==0.17.2', 'streamlit',
    'fastapi', 'uvicorn',
]

setup(
    name='typhoon-orchestrator',
    version=__version__,
    packages=find_packages(),
    install_requires=required,
    extras_require={
        'all': all_requirements,
        'dev': dev_requirements,
        'test': test_required,
    },
    tests_require=test_required,
    test_suite='pytest',
    setup_requires=['wheel'],
    include_package_data=True,
    license='Apache 2 License',
    description='Create asynchronous data pipelines and deploy to cloud or airflow',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/typhoon-data-org/typhoon-orchestrator',
    author='Typhoon Data',
    author_email='info.typhoon.data@gmail.com',
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
