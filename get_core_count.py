"""
Return how many cores are on the node.
"""

import logging

LOGGER = None

def configure_logging(log_level=logging.INFO):
    """
    Configure logging for this script.
    :args log_level: logging level to set
    """
    global LOGGER
    LOGGER = logging.getLogger('get_core_count')
    LOGGER.setLevel(log_level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s.%(funcName)s, line %(lineno)d - \
%(levelname)s - %(message)s'
        )
    console_handler.setFormatter(formatter)
    LOGGER.addHandler(console_handler)
    LOGGER.debug('Starting init of %s', 'get_core_status')

def parse_arguments():
    """
    Parse command line arguments.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description='Returns count of cores on node'
        )
    parser.add_argument(
        '--url', nargs=1, dest='url', required=True,
        type=str,
        help='The hostname or IP of the SolrCloud node to count cores on or\
use to access the rest of the cluster'
        )
    parser.add_argument(
        '--live', action='store_true', required=False,
        help="""Return the core count for live nodes in the cluster"""
        )
    parser.add_argument(
        '--all', action='store_true', required=False,
        help="""Return the core count for all nodes in the cluster, includes those that are down"""
        )
    parser.add_argument(
        '--debug', action='store_true', required=False,
        help="""Turn on debug logging."""
        )
    return parser.parse_args()

def main():
    """
    Called if run from command line.
    """
    args = parse_arguments()

    try:
        from solrcloudadmin import SolrCloudAdmin
    except:
        import sys
        sys.path.append('solrcloudadmin')
        from solrcloudadmin import SolrCloudAdmin

    solr_cloud = SolrCloudAdmin(url=args.url[0], log_level=logging.INFO)

    if args.debug:
        configure_logging(log_level=logging.DEBUG)
    else:
        configure_logging(log_level=logging.INFO)

    if args.live:
        print solr_cloud.pretty_format(solr_cloud.count_live_cores())
    elif args.all:
        print solr_cloud.pretty_format(solr_cloud.count_all_cores())
    else:
        data = solr_cloud.get_core_status()
        print len(data['status'].keys())

if __name__ == '__main__':
    main()
