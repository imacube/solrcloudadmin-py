"""
Delete collection replica.
"""

import logging

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
        '-c', nargs=1, dest='collection', required=True,
        type=str,
        help="""Collection name"""
        )
    parser.add_argument(
        '-s', nargs=1, dest='shard', required=True,
        type=str,
        help="""Shard name"""
        )
    parser.add_argument(
        '-r', nargs=1, dest='replica', required=True,
        type=str,
        help="""Replica name"""
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

    collection = args.collection[0]
    shard = args.shard[0]
    replica = args.replica[0]

    response = solr_cloud.delete_replica(collection=collection, shard=shard, replica=replica)
    solr_cloud.pretty_print(response)

if __name__ == '__main__':
    main()