#!/usr/bin/env python3

"""
Delete a shard's replica from a collection
"""

import sys
import logging
import argparse
from configparser import ConfigParser, ExtendedInterpolation

import requests

sys.path.append('solrcloudadmin')
collections_api import CollectionsApi

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
        '--collection', '-c', nargs=1, dest='collection', required=True,
        type=str,
        help="""Collection to delete shard's replica from"""
        )
    parser.add_argument(
        '--shard', '-s', nargs=1, dest='shard', required=True,
        type=str,
        help="""Collection's shard to delete the replica from"""
        )
    parser.add_argument(
        '--replica', '-r', nargs=1, dest='replica', required=True,
        type=str,
        default=None,
        help="""Collection's shard's replica to delete"""
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
    solr = CollectionsApi(solr_cloud_url=solr_cloud_url, zookeeper_urls=zookeeper_urls, log_level=log_level, timeout=300)

    collection = args.collection[0]
    shard = args.shard[0]
    replica = args.replica[0]

    response = solr.delete_replica(collection=collection, shard=shard, replica=replica)
    try:
        print(response.json())
    except Exception as e:
        print(response.text)
        print(e)

if __name__ == '__main__':
    main()
