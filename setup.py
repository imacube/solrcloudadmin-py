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
    scripts=['scripts/solrcloud_add_replica',
        'scripts/solrcloud_cluster_details',
        'scripts/solrcloud_collection_count',
        'scripts/solrcloud_delete_down',
        'scripts/solrcloud_delete_replica',
        'scripts/solrcloud_list_live_nodes',
        'scripts/solrcloud_migrate_collections',
        'scripts/solrcloud_move_collection',
        'scripts/solrcloud_replica_count',
        'scripts/solrcloud_shard_count',
        'scripts/solrcloud_unhealthy_cores',
        'scripts/solrcloud_view_collections'],
    url='http://github.com/imacube/solrcloudadmin-py',
    license='LICENSE',
    description='utility for running routine admin jobs on a SolrCloud cluster',
    long_description=open('README.md').read() + '\n\n' +
                open('CHANGES.md').read()
)
