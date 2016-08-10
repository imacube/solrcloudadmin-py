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
    scripts=['scripts/add_replica.py',
    'scripts/delete_down.py'
    'scripts/delete_replica.py',
    'scripts/migrate_collections.py',
    'scripts/move_collection.py',
    'scripts/replica_count.py',
    'scripts/unhealthy_cores.py',
    'scripts/view_collections.py'],
    url='http://github.com/imacube/solrcloudadmin-py',
    license='LICENSE',
    description='utility for running routine admin jobs on a SolrCloud cluster',
    long_description=open('README.md').read() + '\n\n' +
                open('CHANGES.md').read()
)
