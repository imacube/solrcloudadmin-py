#!/usr/bin/env python3

"""
Count all the collections on each of the SolrCloud nodes
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
        '--node', nargs=1, dest='node', required=False,
        default=False,
        help="""Return count for a specific node"""
        )
    parser.add_argument(
        '--all', action='store_true', required=False,
        default=False,
        help="""List all nodes, this is a slower option but it captures everything as it queries Zookeeper, the default queries the SolrCloud nodes"""
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
        node = args.node[0]
        print(node, len(solr.get_core_status(node_name=node).json()['status'].keys()))
    elif args.all:
        solrcloud_nodes = dict()
        for collection in tqdm(solr.zookeeper_list_collections(), leave=True):
            response = solr.get_collection_state(collection)
            for shard_name, shard_data in response[0][collection]['shards'].items():
                for replica_name, replica_data in shard_data['replicas'].items():
                    if replica_data['node_name'] in solrcloud_nodes:
                        solrcloud_nodes[replica_data['node_name']] += 1
                    else:
                        solrcloud_nodes[replica_data['node_name']] = 1
        for key in sorted(solrcloud_nodes.keys()):
            print(key, solrcloud_nodes[key])
    else:
        for node in sorted(solr.get_live_solrcloud_nodes()):
            print(node, len(solr.get_core_status(node_name=node).json()['status'].keys()))

if __name__ == '__main__':
    main()
