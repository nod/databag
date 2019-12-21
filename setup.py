#!/usr/bin/env python

import setuptools

with open("README.md") as fh:
    long_description = fh.read()

VERSION = open('VERSION').read().lstrip('version: ').rstrip('\n')

setuptools.setup(
    name='databag',
    version=VERSION,
    description='Put your data in a bag and get it back out again',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    author='Jeremy Kelley',
    author_email='jeremy@33ad.org',
    url='https://github.com/nod/databag',
    packages=['databag'],
    package_dir={'':'src'},
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        "Topic :: Utilities",
        "Topic :: Database",
        ],
    python_requires='>=3.6'
    )
