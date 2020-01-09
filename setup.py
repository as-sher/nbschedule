from setuptools import setup
import os
import sys

_here = os.path.abspath(os.path.dirname(__file__))

version = {}
with open(os.path.join(_here, 'nbschedule', 'version.py')) as f:
    exec(f.read(), version)

setup(
    name='nbschedule',
    version=version['__version__'],
    description=('operators for scheduling notebook'),
    long_description='notebook scheduler',
    license='MPL-2.0',
    packages=['nbschedule'],
#   no dependencies in this example
#   install_requires=[
#       'dependency==1.2.3',
#   ],
#   no scripts in this example
#   scripts=['bin/a-script'],
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6'],
    )
