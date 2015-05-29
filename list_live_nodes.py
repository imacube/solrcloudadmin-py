"""
List live nodes.
"""

import logging

def main():
    """
    Called if run from command line.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description='List Solr nodes in the SolrCloud cluster.'
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

    response = solr_cloud.list_live_nodes()
    solr_cloud.pretty_print(response)

if __name__ == '__main__':
    main()
