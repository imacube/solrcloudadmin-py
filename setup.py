#!/usr/bin/env python

import sys
from setuptools import setup
from solrcloudadmin import __version__

setup(
    name='SolrCloudAdmin',
    version=__version__,
    author='Ryan Steele',
    author_email='imacube@gmail.com',
    packages=[
        'solrcloudadmin'
    ],
    scripts=['scripts/solrcloud_add_replica.py',
        'scripts/solrcloud_list_live_nodes'],
    url='http://github.com/imacube/solrcloudadmin-py',
    license='LICENSE',
    description='utility for running routine admin jobs on a SolrCloud cluster',
    long_description=open('README.md').read() + '\n\n' +
                open('CHANGES.md').read()
)
