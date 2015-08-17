"""
Determine how many replicas are assigned to each Solr node.
"""

from __future__ import print_function
import logging
import json

LOGGER = None

def load_json_file(json_file):
    """
    Load the cluster data from a JSON file.
    """
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
            replica_dict = dict()
            for replica, core in data['replicas'].items():
                replica_dict[replica] = core['node_name']
            collection_count[collection][shard] = replica_dict

    return collection_count

def parse_arguments():
    """
    Parse command line arguments.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description='Get the summary and list of collection replicas across the cluster'
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
        '--node_name', nargs=1, dest='node_name', required=False,
        type=str,
        help="""Only results that have a replica on this node name will be returned."""
        )
    parser.add_argument(
        '--replica_count', nargs=1, dest='replica_count', required=False,
        type=int,
        help="""Only results that have at least replica count will be returned."""
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

    if args.debug:
        configure_logging('get_replica_count', log_level=logging.DEBUG)
    else:
        configure_logging('get_replica_count', log_level=logging.INFO)

    if 'url' in vars(args) and args.url:
        solr_cloud = SolrCloudAdmin(url=args.url[0], log_level=logging.INFO)
    else:
        solr_cloud = None

    if 'json_file' in vars(args) and args.json_file:
        json_file = args.json_file[0]
    else:
        json_file = None

    if 'node_name' in vars(args) and args.node_name:
        filter_node_name = args.node_name[0]
    else:
        filter_node_name = None

    if 'replica_count' in vars(args) and args.replica_count:
        replica_count = args.replica_count[0]
    else:
        replica_count = None

    if not ('url' in vars(args) and args.url) and not\
        ('json_file' in vars(args) and args.json_file):
        LOGGER.critical('Need to set either --url or --json_file')
        return

    LOGGER.info('Loading collection states')
    collection_data = get_collection_data(solr_cloud=solr_cloud, json_file=json_file)

    LOGGER.info('Starting collection replica count')
    count = get_replica_count(collection_data)
    LOGGER.info('Data format:\nreplica count, collection, shard')

    for collection, shards in count.items():
        filter_print = False
        output_string = ''
        for shard, replicas in shards.items():
            if replica_count and len(replicas) < replica_count:
                continue
            output_string += 'Summary: replicas=%d, collection=%s, shard=%s\n' % (
                len(replicas),
                collection,
                shard
                )
            node_name_dict = dict()
            for core, node_name in replicas.items():
                output_string += 'core=%s,node_name=%s\n' % (core, node_name)
                if filter_node_name and node_name == filter_node_name:
                    filter_print = True
                if node_name in node_name_dict:
                    node_name_dict[node_name] += 1
                else:
                    node_name_dict[node_name] = 1
            for node_name, node_replica_count in node_name_dict.items():
                output_string += 'node_name=%s,node_replica_count=%d\n' % (
                    node_name,
                    node_replica_count
                    )
        if filter_node_name and filter_print:
            print(output_string, end='')
        elif not filter_node_name:
            print(output_string, end='')

if __name__ == '__main__':
    main()
