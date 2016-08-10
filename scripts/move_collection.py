#!/usr/bin/env python3

"""
Move a collection off a host to another host
"""

import sys
import pprint
import json
import logging
import re
import argparse
from configparser import ConfigParser, ExtendedInterpolation

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
        description='Move a collection from one host to another'
        )
    parser.add_argument(
        '--collection', '-c', nargs=1, dest='collection', required=True,
        type=str,
        help="""Collection name to move"""
        )
    parser.add_argument(
        '--source-node', '-s', nargs=1, dest='source_node', required=True,
        type=str,
        help="""Node to remove collection from"""
        )
    parser.add_argument(
        '--destination-node', '-d', nargs=1, dest='destination_node', required=False,
        type=str,
        help="""Node to place replica on, if not specified then ranodm node will be choosen"""
        )
    parser.add_argument(
        '--dry-run', '-n', action='store_true', required=False,
        help="""Goes through all the steps except adding the replica"""
        )
    parser.add_argument(
        '--debug', action='store_true', required=False,
        help="""Turn on debug logging."""
        )
    return parser.parse_args()

def move_collection(solr, collection, source_node, destination_node=None):
    """
    Move the specified collection to the new host
    """
    # Get a list of all shards and replicas on this host
    collection_state = solr.get_collection_state(collection=collection)[0][collection]

    # Add replicas for all the shards on this host to other hosts
    for shard, replicas in collection_state['shards'].items():
        for replica_name, replica_data in replicas['replicas'].items():
            if replica_data['node_name'] == source_node:
                result = solr.add_replica(collection=collection, shard=shard, node=destination_node)
                if 'success' not in result.json():
                    raise Exception({'Msg': 'success not found in result', 'result': result.content})
                else:
                    print('replica added')
                    print('result: {}'.format(result.json()['success']))
    # Delete the collection off this host
    collection_state = solr.get_collection_state(collection=collection)[0][collection]
    for shard, replicas in collection_state['shards'].items():
        for replica_name, replica_data in replicas['replicas'].items():
            if replica_data['node_name'] == source_node:
                print(replica_name, replica_data)
                result = solr.delete_replica(collection=collection, shard=shard, replica=replica_name)
                print(result.json())

def main():
    """
    Main function to be called if run as a script
    """
    # Load configuration file
    config = load_configuation_files()
    # Load command line arguments
    args = parse_arguments()

    solr_cloud_url = '%s' % (config['solr']['host'])
    zookeeper_urls = '%s' % (config['zookeeper']['host'])

    log_level = logging.INFO
    if args.debug:
        log_level=logging.DEBUG

    # Process source node

    # Configure solr library
    solr = CollectionsApi(solr_cloud_url=solr_cloud_url, zookeeper_urls=zookeeper_urls, log_level=log_level)
    move_collection(solr=solr, collection=args.collection[0], source_node=args.source_node[0])

if __name__ == '__main__':
    main()
