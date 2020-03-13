from setuptools import setup, find_packages
import os
import sys

_here = os.path.abspath(os.path.dirname(__file__))

version = {}
with open(os.path.join(_here, 'nbschedule', 'version.py')) as f:
    exec(f.read(), version)

python_2 = sys.version_info[0] == 2

def read(fname):
    with open(fname, 'rU' if python_2 else 'r') as fhandle:
        return fhandle.read()


def read_reqs(fname):
    req_path = os.path.join(_here, fname)
    return [req.strip() for req in read(req_path).splitlines() if req.strip()]

all_reqs = read_reqs('requirements.txt')

setup(
    name='nbschedule',
    version=version['__version__'],
    description=('operators for scheduling notebook'),
    long_description='notebook scheduler',
    license='MPL-2.0',
    packages=find_packages(),
    install_requires= all_reqs,
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6'],
)
