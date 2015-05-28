"""
Query request ID.
"""

import logging

def main():
    """
    Called if run from command line.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description='Queries request ID and prints JSON status'
        )
    parser.add_argument(
        '--url', nargs=1, dest='url', required=True,
        type=str,
        help="""The hostname or IP of the SolrCloud cluster, e.g. solrcloud:8983/solr"""
        )
    parser.add_argument(
        '-i', nargs=1, dest='request_id', required=True,
        type=str,
        help="""The request ID to be queried."""
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

    request_id = args.request_id[0]

    data = solr_cloud.get_request_status(request_id)
    solr_cloud.pretty_print(data)

if __name__ == '__main__':
    main()
