#!/usr/bin/env python

from distutils.core import setup

VERSION = open('VERSION').read().lstrip('version: ').rstrip('\n')

setup(
    name='databag',
    version=VERSION,
    description='Put your data in a bag and get it back out again',
    long_description=open('README.rst').read(),
    author='Jeremy Kelley',
    author_email='jeremy@33ad.org',
    url='https://github.com/nod/databag',
    packages=['databag', 'databag.contrib'],
    classifiers=(
        'License :: OSI Approved :: MIT License',
        "Development Status :: 4 - Beta",
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        "Topic :: Utilities",
        "Topic :: Database",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.2",
        ),
    )
