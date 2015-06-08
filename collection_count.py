"""
Print count of collections per SolrCloud node.
"""

import logging

LOGGER = None

def configure_logging(log_level=logging.INFO):
    """
    Configure logging for this script.
    :args log_level: logging level to set
    """
    global LOGGER
    LOGGER = logging.getLogger('collection_count')
    LOGGER.setLevel(log_level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s.%(funcName)s, line %(lineno)d - \
%(levelname)s - %(message)s'
        )
    console_handler.setFormatter(formatter)
    LOGGER.addHandler(console_handler)
    LOGGER.debug('Starting init of %s', 'collection_count')

def parse_arguments():
    """
    Parse command line arguments.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description='Print count of collections per SolrCloud node.'
        )
    parser.add_argument(
        '--url', nargs=1, dest='url', required=True,
        type=str,
        help="""The hostname or IP of the SolrCloud cluster, e.g. solrcloud:8983/solr"""
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
        configure_logging(log_level=logging.DEBUG)
    else:
        configure_logging(log_level=logging.INFO)
    solr_cloud = SolrCloudAdmin(url=args.url[0], log_level=logging.INFO)

    response = solr_cloud.collection_count()
    solr_cloud.pretty_print(response)

if __name__ == '__main__':
    main()
