#!/usr/bin/env python3

"""
Get the number of shards for each collection, or the specified collection
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
        description='Get shard count for every colleciton or a specific one'
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
        help="""Return count of shards for collection"""
        )
    parser.add_argument(
        '--lt', nargs=1, dest='lt', required=False,
        default=False,
        help="""Return count of shards if less than this value"""
        )
    parser.add_argument(
        '--gt', nargs=1, dest='gt', required=False,
        default=False,
        help="""Return count of chards if greater than this value"""
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

    final_list = list()
    for collection in tqdm(collection_list, leave=True):
        shard_count = len(solr.get_collection_state(collection)[0][collection]['shards'])
        if not args.lt and not args.gt:
            final_list.append((collection, shard_count))
            # print(collection, shard_count)
        if args.lt:
            if shard_count < int(args.lt[0]):
                # print(collection, shard_count)
                final_list.append((collection, shard_count))
        if args.gt:
            if shard_count > int(args.gt[0]):
                # print(collection, shard_count)
                final_list.append((collection, shard_count))

    for collection in final_list:
        print('{}, {}'.format(collection[0], collection[1]))

if __name__ == '__main__':
    main()
