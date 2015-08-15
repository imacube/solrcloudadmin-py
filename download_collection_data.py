"""
Download all collection data into a file.
"""

import logging
import json

LOGGER = None

def configure_logging(log_level=logging.INFO):
    """
    Configure logging for this script.
    :args log_level: logging level to set
    """
    global LOGGER
    logger_name = 'download_collection_data'
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
    import argparse
    parser = argparse.ArgumentParser(
        description='Delete the specified collection replica.'
        )
    parser.add_argument(
        '--url', nargs=1, dest='url', required=True,
        type=str,
        help="""The hostname or IP of the SolrCloud cluster, e.g. solrcloud:8983/solr"""
        )
    parser.add_argument(
        '--json_file', nargs=1, dest='json_file', required=True,
        type=str,
        help="""The JSON file to write the data to."""
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

    json_file = args.json_file[0]

    LOGGER.info('Starting download of data')
    data = solr_cloud.list_collection_data()
    LOGGER.info('Writing JSON to file %s', json_file)
    with open(json_file, 'w') as out_file:
        json.dump(data, out_file)
    LOGGER.info('Done')

if __name__ == '__main__':
    main()