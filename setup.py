#!/usr/bin/env python

import setuptools

long_descr = open('README.md').read()
VERSION = open('VERSION').read().strip()

setuptools.setup(
    name='databag',
    version=VERSION,
    description='Put your data in a bag and get it back out again',
    long_description=long_descr,
    long_description_content_type="text/markdown",
    author='Jeremy Kelley',
    author_email='jeremy@33ad.org',
    url='https://github.com/nod/databag',
    packages=['databag', 'databag.orm'],
    package_dir={'':'src'},
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        "Topic :: Utilities",
        "Topic :: Database",
        ],
    python_requires='>=3.6'
    )

