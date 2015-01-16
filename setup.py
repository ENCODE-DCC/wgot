#!/usr/bin/env python
import sys
from setuptools import setup

requires = [
    'requests',
]

if sys.version_info[:2] == (2, 6):
    # For python2.6 we have to require argparse since it
    # was not in stdlib until 2.7.
    requires.append('argparse>=1.1')


setup_options = dict(
    name='paraget',
    version='0.1',
    description='Peformant parallel http downloading extracted from aws-cli.',
    long_description=open('README.rst').read(),
    author='Laurence Rowe',
    author_email='l@lrowe.co.uk',
    packages=['paraget'],
    install_requires=requires,
    license="Apache License 2.0",
    classifiers=(
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
    ),
    entry_points='''
        [console_scripts]
        paraget = paraget.command:main
    ''',
)

setup(**setup_options)
