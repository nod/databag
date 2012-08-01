#!/usr/bin/env python

from distutils.core import setup

VERSION = open('VERSION').read().lstrip('version: ').rstrip('\n')

setup(
    name='databag',
    version=VERSION,
    description='Put your data in a bag and get it back out again',
    author='Jeremy Kelley',
    author_email='jeremy@33ad.org',
    url='https://github.com/nod/databag',
    packages=['databag', 'databag.contrib'],
    )
