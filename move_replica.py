"""
Move replica to new host.
"""

import logging

LOGGER = None

def configure_logging(log_level=logging.INFO):
    """
    Configure logging for this script.
    :args log_level: logging level to set
    """
    global LOGGER
    LOGGER = logging.getLogger('move_replica')
    LOGGER.setLevel(log_level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s.%(funcName)s, line %(lineno)d - \
%(levelname)s - %(message)s'
        )
    console_handler.setFormatter(formatter)
    LOGGER.addHandler(console_handler)
    LOGGER.debug('Starting init of %s', 'move_replica')

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
        '-c', nargs=1, dest='collection', required=True,
        type=str,
        help="""The collection whose replica needs to be moved."""
        )
    parser.add_argument(
        '--shard', nargs=1, dest='shard', required=True,
        type=str,
        help="""The shard to move."""
        )
    parser.add_argument(
        '-s', nargs=1, dest='source', required=True,
        type=str,
        help="""The source host as SolrCloud formated node_name."""
        )
    parser.add_argument(
        '-d', nargs=1, dest='destination', required=True,
        type=str,
        help="""The destination host as SolrCloud formated node_name."""
        )
    parser.add_argument(
        '--async', nargs=1, dest='async', required=False,
        type=int,
        help="""Async request ID."""
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
        configure_logging(log_level=logging.DEBUG)
        solr_cloud = SolrCloudAdmin(url=args.url[0], log_level=logging.DEBUG)
    else:
        configure_logging(log_level=logging.INFO)
        solr_cloud = SolrCloudAdmin(url=args.url[0], log_level=logging.INFO)

    collection = args.collection[0]
    shard = args.shard[0]
    source = args.source[0]
    destination = args.destination[0]
    async = None

    if 'async' in vars(args):
        async = args.async[0]
        LOGGER.debug('async = ', async)

    data = solr_cloud.move_shard(
        collection=collection,
        shard=shard,
        source_node=source,
        destination_node=destination,
        async=async
        )
    solr_cloud.pretty_print(data)

if __name__ == '__main__':
    main()
