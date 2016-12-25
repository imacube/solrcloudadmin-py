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
    install_requires=[
        'kazoo>=2.2.1',
        'requests>=2.11.1',
        'tqdm>=4.8.4'
    ],
    scripts=['scripts/solrcloud-add-replica',
             'scripts/solrcloud-cluster-details',
             'scripts/solrcloud-cluster-recovery',
             'scripts/solrcloud-collection-count',
             'scripts/solrcloud-delete-down',
             'scripts/solrcloud-delete-replica',
             'scripts/solrcloud-document-count.py',
             'scripts/solrcloud-list-live-nodes',
             'scripts/solrcloud-migrate-collections',
             'scripts/solrcloud-move-collection',
             'scripts/solrcloud-replica-count',
             'scripts/solrcloud-shard-count',
             'scripts/solrcloud-unhealthy-cores',
             'scripts/solrcloud-view-collections'
             ],
    url='http://github.com/imacube/solrcloudadmin-py',
    license='LICENSE',
    description='utility for running routine admin jobs on a SolrCloud cluster',
    long_description=open('README.md').read() + '\n\n' +
                open('CHANGES.md').read()
)
