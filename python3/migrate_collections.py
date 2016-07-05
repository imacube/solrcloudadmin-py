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
from concurrent import futures

sys.path.append('../solr_cloud_collections_api')
from solr_cloud_collections_api import SolrCloudCollectionsApi

class MoveCollections(object):
    """
    Used to move collections
    """
    def __init__(self, solr, source_node, destination_node=None, log_level=logging.INFO):
        self.solr = solr
        self.source_node = source_node
        self.destination_node = destination_node
        self.max_workers = 5

        logger = logging.getLogger(__name__)
        logger.setLevel(log_level)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s.%(funcName)s, line %(lineno)d - \
%(levelname)s - %(message)s'
            )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        self.logger = logger
        self.logger.debug('Init complete for {}'.format(__name__))

    def build_colleciton_list(self):
        """
        Build a list of collections on host
        """
        collection_list = self.solr.get_collections_on_node(node_name=self.source_node)
        collection_list = 'ryan-test0 ryan-test1 ryan-test2 ryan-test3 ryan-test4'.split()  # todo: delete when ready to use for real, limits migration to single collection
        return collection_list

    def move_collection(self, collection):
        """
        Move the specified collection to the new host
        """
        # Get a list of all shards and replicas on this host
        collection_state = self.solr.get_collection_state(collection=collection)[0][collection]
        self.logger.debug('collection_state={}'.format(collection_state))

        # Add replicas for all the shards on this host to other hosts
        for shard, replicas in collection_state['shards'].items():
            for replica_name, replica_data in replicas['replicas'].items():
                print(replica_data['node_name'], self.source_node)
                if replica_data['node_name'] == self.source_node:
                    result = self.solr.add_replica(collection=collection, shard=shard, node=self.destination_node)
                    if 'success' not in result.json():
                        raise Exception({'Msg': 'success not found in result', 'result': result.content})
                    else:
                        print('result: {}'.format(result.json()['success']))
        # Delete the collection off this host
        collection_state = self.solr.get_collection_state(collection=collection)[0][collection]
        for shard, replicas in collection_state['shards'].items():
            for replica_name, replica_data in replicas['replicas'].items():
                if replica_data['node_name'] == self.source_node:
                    self.logger.debug('replica_name={} replica_data={}'.format(replica_name, replica_data))
                    result = self.solr.delete_replica(collection=collection, shard=shard, replica=replica_name)
                    self.logger.info('Deleted {} {} {}'.format(collection, shard, replica_name))
                    self.logger.debug(result.json())

    def migrate_collections(self):
        """
        Move all collections off the source_node to an optional destination_node
        """
        collection_list = self.build_colleciton_list()

        workers = min(self.max_workers, len(collection_list))
        with futures.ThreadPoolExecutor(workers) as executor:
            res = executor.map(self.move_collection, collection_list)
        return len(list(res))

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

    source_node = args.source_node[0]
    destination_node = None
    if args.destination_node is not None:
        destination_node = args.destination_node[0]

    # Configure solr library
    solr = SolrCloudCollectionsApi(solr_cloud_url=solr_cloud_url, zookeeper_urls=zookeeper_urls, log_level=log_level, timeout=60)
    move_collections = MoveCollections(solr=solr, source_node=source_node, destination_node=destination_node, log_level=log_level)
    result = move_collections.migrate_collections()
    print(result)

if __name__ == '__main__':
    main()
