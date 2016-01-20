#!/usr/bin/env python
from setuptools import find_packages, setup

__version__ = "0.0.1"

_console_scripts = [
    'peloton-agent = peloton.agent.main:serve',
    'peloton-master-tornado = peloton.master.main:serve',
]

setup(
    name="peloton",
    version=__version__,
    author='Uber Technologies.',
    description='Peloton System',
    url='gitolite@code.uber.internal:infra/peloton',
    packages=find_packages(exclude=['*.tests']),
    entry_points={
        'console_scripts': _console_scripts
    }
)
