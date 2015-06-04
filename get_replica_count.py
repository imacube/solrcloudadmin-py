"""
Determine how many replicas are assigned to each Solr node.
"""

import logging
import json

LOGGER = None

def load_json_file(json_file):
    """
    Load the cluster data from a JSON file.
    """
    import json
    with open(json_file) as data_file:
        data = json.load(data_file)
    return data

def get_collection_data(solr_cloud=None, json_file=None):
    """
    Load the collection data either from a file
    or from the SolrCloud cluster.
    """
    if json_file:
        return load_json_file(json_file=json_file)
    elif solr_cloud:
        return solr_cloud.list_collection_data()

def get_replica_count(collection_data):
    """
    Get the count of replicas each collection has.
    """
    collection_count = dict()
    for collection, value in collection_data.items():
        collection_count[collection] = dict()
        for shard, data in value['shards'].items():
            collection_count[collection][shard] = len(data['replicas'])
    return collection_count

def parse_arguments():
    """
    Parse command line arguments.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description='Delete the specified collection replica.'
        )
    parser.add_argument(
        '--url', nargs=1, dest='url', required=False,
        type=str,
        help="""The hostname or IP of the SolrCloud cluster, e.g. solrcloud:8983/solr"""
        )
    parser.add_argument(
        '--json_file', nargs=1, dest='json_file', required=False,
        type=str,
        help="""The JSON file to read the data from."""
        )
    parser.add_argument(
        '--debug', action='store_true', required=False,
        help="""Turn on debug logging."""
        )
    parser.add_argument(
        '--local_solrcloudadmin', action='store_true', required=False,
        help="""Import SolrCloudAdmin from local directory."""
        )
    return parser.parse_args()

def configure_logging(logger_name, log_level=logging.INFO):
    """
    Configure logging for this script.
    :args log_level: logging level to set
    """
    global LOGGER
    LOGGER = logging.getLogger(logger_name)
    LOGGER.setLevel(log_level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s.%(funcName)s, line %(lineno)d - \
%(levelname)s - %(message)s'
        )
    console_handler.setFormatter(formatter)
    LOGGER.addHandler(console_handler)
    LOGGER.debug('Starting init of %s', logger_name)

def main():
    """
    Called if run from command line.
    """
    args = parse_arguments()

    if args.local_solrcloudadmin:
        import sys
        sys.path.append('solrcloudadmin')
        from solrcloudadmin import SolrCloudAdmin

    solr_cloud = None
    if args.debug:
        configure_logging('get_replica_count', log_level=logging.DEBUG)
    else:
        configure_logging('get_replica_count', log_level=logging.INFO)

    if 'url' in vars(args):
        if args.url:
            solr_cloud = SolrCloudAdmin(url=args.url[0], log_level=logging.INFO)

    if 'json_file' in vars(args):
        if args.json_file:
            json_file = args.json_file[0]

    LOGGER.info('Loading collection states')
    collection_data = get_collection_data(solr_cloud=solr_cloud, json_file=json_file)

    LOGGER.info('Starting collection replica count')
    count = get_replica_count(collection_data)
    LOGGER.info('Data format:\nreplica count, collection, shard')
    for collection, shards in count.items():
        for shard, replica_count in shards.items():
            print '%s,%s,%s' % (replica_count, collection, shard)

if __name__ == '__main__':
    main()
