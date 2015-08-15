"""
Return information for a specified core.
"""

import logging

LOGGER = None

def configure_logging(log_level=logging.INFO):
    """
    Configure logging for this script.
    :args log_level: logging level to set
    """
    global LOGGER
    LOGGER = logging.getLogger('get_core_status')
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

def main():
    """
    Called if run from command line.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description='Returns core information.'
        )
    parser.add_argument(
        '--url', nargs=1, dest='url', required=True,
        type=str,
        help="""The hostname or IP of the SolrCloud cluster, e.g. solrcloud:8983/solr"""
        )
    parser.add_argument(
        '-c', nargs=1, dest='core', required=True,
        type=str,
        help="""The core to lookup."""
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

    core = args.core[0]

    response = solr_cloud.get_core_status(core)
    solr_cloud.pretty_print(response)

if __name__ == '__main__':
    main()