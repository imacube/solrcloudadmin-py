#!/usr/bin/env python

import sys
from setuptools import setup
from solrcloudadmin import __version__

setup(
    name='SolrCloudAdmin',
    version=__version__,
    author='Ryan Steele',
    author_email='ryan@imacube.net',
    packages=[
        'solrcloudadmin'
    ],
    scripts=['scripts/add_replica',
    'scripts/delete_replica',
    'scripts/download_collection_data',
    'scripts/get_collection_state',
    'scripts/get_core_count',
    'scripts/get_core_status',
    'scripts/get_replica_count',
    'scripts/list_live_nodes',
    'scripts/migrate_collections',
    'scripts/move_replica',
    'scripts/query_request_id'],
    url='http://github.com/imacube/solrcloudadmin-py',
    license='LICENSE',
    description='utility for running routine admin jobs on a SolrCloud cluster',
    long_description=open('README.md').read() + '\n\n' +
                open('CHANGES.md').read()
)
