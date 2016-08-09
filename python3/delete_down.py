#!/usr/bin/env python3

"""
Delete replicas that are down, useful for when a node is gone
"""

import sys
import logging
import argparse
from configparser import ConfigParser, ExtendedInterpolation

import requests

sys.path.append('../solr_cloud_collections_api')
from solr_cloud_collections_api import SolrCloudCollectionsApi

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
        description='Delete replicas that are in a down state, useful for cleaning up replicas on a SolrCloud node that no longer exists'
        )
    parser.add_argument(
        '--config', nargs=1, dest='config', required=False,
        type=str,
        default=['config.ini'],
        help="""Configuration file to load"""
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
    solr = SolrCloudCollectionsApi(solr_cloud_url=solr_cloud_url, zookeeper_urls=zookeeper_urls, log_level=log_level)

    collection_list = None
    if args.collection:
        collection_list = args.collection
    else:
        collection_list = solr.zookeeper_list_collections()

    for collection in collection_list:
        for shard_name, shard_data in solr.get_collection_state(collection)[0][collection]['shards'].items():
            if shard_data['state'] != 'active':
                print('''Won't work on shards that aren't active''', collection, shard_name, shard_data)
                break

            can_delete = False
            to_delete = list()
            for replica_name, replica_data in shard_data['replicas'].items():
                if replica_data['state'] == 'down':
                    to_delete.append(replica_name)
                elif replica_data['state'] == 'active':
                    can_delete = True

            if can_delete:
                print(collection, shard_name, to_delete)
                for replica in to_delete:
                    response = solr.delete_replica(collection=collection, shard=shard_name, replica=replica)
                    print(response.raw)
            else:
                print(collection)

if __name__ == '__main__':
    main()
