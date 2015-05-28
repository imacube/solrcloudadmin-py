"""
Migrate collections from one host to another.
"""

import logging

def find_collection(solr_cloud, source_node):
    """
    Find a collection that needs to be moved.
    """
    for collection in solr_cloud.list_collections_only():
        data = solr_cloud.get_collection_state(collection=collection)
        for shard in data['shards']:
            for replica in data['shards'][shard]['replicas']:
                if data['shards'][shard]['replicas'][replica]['node_name'] == source_node:
                    return_dict = {
                        'collection': collection,
                        'shard': shard,
                        'replica': replica
                        }
                    logging.debug('Found match: %s', solr_cloud.pretty_format(return_dict))
                    return return_dict

def main():
    """
    Called if run from command line.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description='Moves collection replica from one node to another'
        )
    parser.add_argument(
        '--url', nargs=1, dest='url', required=True,
        type=str,
        help="""The hostname or IP of the SolrCloud cluster, e.g. solrcloud:8983/solr"""
        )
    parser.add_argument(
        '-s', nargs=1, dest='source', required=True,
        type=str,
        help="""The source host as SolrCloud formated node_name."""
        )
    parser.add_argument(
        '-d', nargs=1, dest='destination', required=False,
        type=str,
        help="""The destination host as SolrCloud formated node_name."""
        )
    parser.add_argument(
        '--limit', nargs=1, dest='limit', required=True,
        type=int,
        help="""The limit of how many collections to move. Defaults to all."""
        )
    parser.add_argument(
        '--debug', action='store_true', required=False,
        help="""Turn on debug logging."""
        )
    parser.add_argument(
        '--local_solrcloudadmin', action='store_true', required=False,
        help="""Import SolrCloudAdmin from local directory."""
        )
    args = parser.parse_args()

    if args.local_solrcloudadmin:
        import sys
        sys.path.append('solrcloudadmin')
        from solrcloudadmin import SolrCloudAdmin

    solr_cloud = None
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        solr_cloud = SolrCloudAdmin(url=args.url[0], loglevel=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
        solr_cloud = SolrCloudAdmin(url=args.url[0], loglevel=logging.INFO)

    source_node = args.source[0]
    destination_node = None
    limit = 0

    if 'destination_node' in vars(args):
        destination_node = args.destination[0]
        logging.debug('destination_node: %s', destination_node)
    if 'limit' in vars(args):
        limit = args.limit[0]
        logging.debug('limit: %s', limit)

    count = 0
    while limit == 0 or count < limit:
        logging.debug('count: %d', count)
        logging.debug('limit: %d', limit)
        # Find collection to move
        data = find_collection(solr_cloud, source_node)

        if data is None:
            logging.info('No more collections found to migrate')
            break

        # Move that collection
        logging.debug('Moving:\n%s', solr_cloud.pretty_format(data))
        logging.info('Moving: %s %s %s', data['collection'], data['shard'], data['replica'])
        data = solr_cloud.move_replica(
            collection=data['collection'],
            shard=data['shard'],
            replica=data['replica'],
            destination_node=destination_node
            )
        if data['status'] != 0:
            logging.critical(solr_cloud.pretty_format(data))
            break
        logging.debug('move_replica response:\n%s', solr_cloud.pretty_format(data['response']))

        # Repeat as limit allows
        if limit > 0:
            count += 1

if __name__ == '__main__':
    main()
