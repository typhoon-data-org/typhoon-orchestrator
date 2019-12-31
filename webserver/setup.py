from setuptools import setup, find_packages

from typhoon_webserver import __version__

required = ['typhoon-orchestrator<=0.0.3', 'flask']
test_required = ['pytest']


setup(
    name='typhoon-webserver',
    version=__version__,
    packages=find_packages(),
    install_requires=required,
    extras_require={
        'all': required,
    },
    tests_require=test_required,
    test_suite='pytest',
    include_package_data=True,
    license='MIT License',
    description='Optional companion web server for Typhoon Orchestrator',
    long_description='Optional companion web server for Typhoon Orchestrator',
    long_description_content_type='text/markdown',
    url='https://github.com/biellls/typhoon-orchestrator',
    author='Gabriel Llobera Salas',
    author_email='bielllobera@gmail.com',
    classifiers=[
        'Environment :: Console',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
    ],
)
