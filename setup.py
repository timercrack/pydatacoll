#!/usr/bin/env python
#
# Copyright 2016 timercrack
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from setuptools import setup, find_packages
import sys
from pydatacoll import version
if sys.version_info < (3, 5):
    print("PyDataColl needs python version >= 3.5, please upgrade!")
    sys.exit(1)

kwargs = {}

with open('README.rst') as f:
    kwargs['long_description'] = f.read()

kwargs['version'] = version

kwargs['install_requires'] = [
    'aiohttp>=0.20.1',
    'aioredis>=0.2.4',
    'cchardet>=1.0.0',
    'construct>=2.5.2',
    'hiredis>=0.2.0',
    'numpy>=1.10.4',
    'pandas>=0.17.1',
    'PyMySQL>=0.6.7',
    'python-dateutil>=2.4.2',
    'pytz>=2015.7',
    'redis>=2.10.5',
    'six>=1.10.0',
    'ujson>=1.34',
    'appdirs>=1.4.0',
]

setup(
    name="PyDataColl",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'pydatacoll = pydatacoll.api_server:main',
        ],
        'setuptools.installation': [
            'eggsecutable = pydatacoll.api_server:main',
        ]
    },
    keywords="IEC60870 SCADA EMS",
    author="timercrack",
    author_email="timercrack@gmail.com",
    url="http://pydatacoll.readthedocs.org/",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    description="PyDataColl is a SCADA-like system, originally developed at GDT.",
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Intended Audience :: Information Technology',
        'Natural Language :: English',
        'Natural Language :: Chinese (Simplified)',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
    ],
    **kwargs
)
