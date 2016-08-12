#!/usr/bin/env python3

"""
Find cores with problems
"""

import sys
import logging
import argparse
from configparser import ConfigParser, ExtendedInterpolation

import requests
from tqdm import *

sys.path.append('../solrcloudadmin')
from collections_api import CollectionsApi

def load_configuation_files(
    general_configuration='config.ini'):
    config = ConfigParser(interpolation=ExtendedInterpolation())
    if len(config.read(general_configuration)) == 0:
        print('Unable to load %s' % general_configuration)
        sys.exit(1)
    return config

def parse_arguments():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Get colleciton count for each live node in the cluster'
        )
    parser.add_argument(
        '--config', nargs=1, dest='config', required=False,
        type=str,
        default=['config.ini'],
        help="""Configuration file to load"""
        )
    parser.add_argument(
        '--collection', '-c', nargs=1, dest='collection', required=False,
        default=False,
        help="""Return count of replicas for collection"""
        )
    parser.add_argument(
        '--debug', action='store_true', required=False,
        help="""Turn on debug logging."""
        )
    return parser.parse_args()

def main():
    """
    Main function to be called if run as a script
    """
    # Load command line arguments
    args = parse_arguments()
    # Load configuration file
    config = load_configuation_files(args.config[0])

    solr_cloud_url = '%s' % (config['solr']['host'])
    zookeeper_urls = '%s' % (config['zookeeper']['host'])

    log_level = logging.INFO
    if args.debug:
        log_level=logging.DEBUG

    # Configure solr library
    solr = CollectionsApi(solr_cloud_url=solr_cloud_url, zookeeper_urls=zookeeper_urls, log_level=log_level)

    collection_list = None
    if args.collection:
        collection_list = args.collection
    else:
        collection_list = solr.zookeeper_list_collections()

    unhealthy_collections = list()

    for collection in tqdm(collection_list, leave=True):
        for shard_name, shard_data in solr.get_collection_state(collection)[0][collection]['shards'].items():
            if shard_data['state'] != 'active':
                unhealthy_collections.append((collection, shard_name, shard_data))
                break
            for replica_name, replica_data in shard_data['replicas'].items():
                if replica_data['state'] != 'active':
                    unhealthy_collections.append((collection, shard_name, replica_name, replica_data))
    for collection in unhealthy_collections:
        print(collection)

if __name__ == '__main__':
    main()
