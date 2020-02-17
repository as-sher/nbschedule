from setuptools import setup, find_packages
import os
import sys
import pip


_here = os.path.abspath(os.path.dirname(__file__))

version = {}
with open(os.path.join(_here, 'nbschedule', 'version.py')) as f:
    exec(f.read(), version)

def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])

install("git+https://github.com/as-sher/sparkmagic.git#subdirectory=hdijupyterutils")
install("git+https://github.com/as-sher/sparkmagic.git#subdirectory=autovizwidget")
install("git+https://github.com/as-sher/sparkmagic.git#subdirectory=sparkmagic")


setup(
    name='nbschedule',
    version=version['__version__'],
    description=('operators for scheduling notebook'),
    long_description='notebook scheduler',
    license='MPL-2.0',
    packages=find_packages(),
#   no dependencies in this example
    install_requires=[
        "ipykernel==5.1.3",
    ],
    dependency_links = [
    ],
#   no scripts in this example
#   scripts=['bin/a-script'],
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6'],
    )
