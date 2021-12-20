"""Typhoon is a task orchestrator and workfow manager used to create asynchronous data pipelines that can be deployed to AWS Lambda/Fargate to be completely serverless."""
import sys

SUPPORTED_VERSIONS = [(3, 6)]

python_major_version = sys.version_info[0]
python_minor_version = sys.version_info[1]

# if (python_major_version, python_minor_version) not in SUPPORTED_VERSIONS:
#     formatted_supported_versions = ['{}.{}'.format(mav, miv) for mav, miv in SUPPORTED_VERSIONS]
#     err_msg = 'This version of Python ({}.{}) is not supported!\n'.format(python_major_version, python_minor_version)
#     raise RuntimeError(err_msg)

__version__ = '0.0.44'
