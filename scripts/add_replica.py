#!/usr/bin/env python3

"""
Add a replica to a collection
"""

import sys
import logging
import argparse
from configparser import ConfigParser, ExtendedInterpolation

import requests

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
        '--collection', '-c', nargs=1, dest='collection', required=True,
        default=False,
        help="""Return count of replicas for collection"""
        )
    parser.add_argument(
        '--shard', '-s', nargs=1, dest='shard', required=True,
        default=False,
        help="""Collection's shard to add a replica to"""
        )
    parser.add_argument(
        '--node', nargs=1, dest='node', required=False,
        default=False,
        help="""SolrCloud node to add the replica to"""
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

    if args.node:
        response = solr.add_replica(args.collection[0], shard=args.shard[0], node=args.node[0])
    else:
        response = solr.add_replica(args.collection[0], shard=args.shard[0], node=None)
    try:
        print(response.json())
    except Exception as e:
        print(response.text)
        print(e)

if __name__ == '__main__':
    main()
