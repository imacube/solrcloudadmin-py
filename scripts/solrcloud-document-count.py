"""
Counts documents in a collection
"""

import argparse
import json
import logging
import sys
from time import sleep

from kazoo.client import KazooClient
import requests

from solrcloudadmin.collections_api import CollectionsApi
from solrcloudadmin.scripts_library import ScriptsLibrary

SCRIPTS_LIBRARY = ScriptsLibrary()

def get_solrcloud_node_urls(collection_data):
    """
    Find a URL for one of the SolrCloud nodes that has
    a replica on it.
    """
    core_list = dict()

    for collection in collection_data.values():
        for shard_name, shard_data in collection['shards'].items():
            if shard_data['state'] != 'active':
                continue
            for replica_data in shard_data['replicas'].values():
                if replica_data['state'] != 'active':
                    continue
                return replica_data['base_url']
    return None

def parse_arguments():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Get count of documents in a collection'
        )
    parser = SCRIPTS_LIBRARY.additional_arguments(parser)
    parser.add_argument(
        '--collection', '-c', nargs=1, dest='collection', required=True,
        default=False,
        help="""Return count of documents in this collection"""
        )
    return parser.parse_args()

def main():
    """
    Main function to be called if run as a script
    """
    # Load command line arguments
    args = parse_arguments()
    # Load configuration file
    config = SCRIPTS_LIBRARY.load_configuation_files(args.config[0])

    profile = args.profile[0]
    solr_cloud_url = '%s' % (config[profile]['solrcloud'])
    zookeeper_urls = '%s' % (config[profile]['zookeeper'])

    log_level = logging.INFO
    if args.debug:
        log_level=logging.DEBUG

    # Configure solr library
    solr = CollectionsApi(solr_cloud_url=solr_cloud_url, zookeeper_urls=zookeeper_urls, log_level=log_level, timeout=300)

    collection = args.collection[0]
    collection_data = solr.get_collection_state(collection=collection)[0]

    parameters = {'q': '*:*', 'wt': 'json', 'indent': 'true', 'rows': 1}
    base_url = get_solrcloud_node_urls(collection_data)
    response = solr.make_get_request(path='/{}/select'.format(collection), parameters=parameters, solr_cloud_url=base_url)
    print('Document count: {}'.format(response.json()['response']['numFound']))

if __name__ == '__main__':
    main()
