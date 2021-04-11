import sys

SUPPORTED_VERSIONS = [(3, 6), (3, 7), (3, 8)]

python_major_version = sys.version_info[0]
python_minor_version = sys.version_info[1]

__typhoon_extension__ = True
