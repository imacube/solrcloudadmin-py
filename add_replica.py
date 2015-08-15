"""
Add collection replica.
"""

import logging
import time

LOGGER = None

def check_node_live(solr_cloud, collection, shard, node_to_check):
    """
    Pulls the current collection state.json and checks if the destination_node
    node already has a copy of the shard.
    """
    if node_to_check is None:
        return False
    shard_data = solr_cloud.get_collection_state(collection=collection)['shards'][shard]
    for replica, core in shard_data['replicas'].items():
        LOGGER.debug('replica=%s', replica)
        LOGGER.debug('node_name=%s', core['node_name'])
        LOGGER.debug('node_to_check=%s', node_to_check)
        if node_to_check == core['node_name']:
            return True
    return False

def wait_for_async(solr_cloud, async):
    """
    Wait for an async job to finish.
    """
    failed_queries = 0
    while True:
        if failed_queries > 10:
            LOGGER.critical('failed_queries=%d > 10', failed_queries)
            return False
        response = solr_cloud.get_request_status(async)
        LOGGER.debug('response = %s', response)
        if response['status']['state'] == 'running':
            LOGGER.debug('Waiting for job to finish...')
            failed_queries = 0
        elif response['status']['state'] == 'submitted':
            LOGGER.debug('Waiting for job to start...')
            failed_queries = 0
        elif response['status']['state'] == 'notfound':
            LOGGER.warn('Did not find task for async ID %d.', async)
            failed_queries += 1
        elif response['status']['state'] == 'completed':
            return True
        else:
            LOGGER.critical('Unexpected task state, response = %s', response)
            return False
        time.sleep(2)

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

def parse_arguments():
    """
    Parse command line arguments.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description='Add a replica of a shard.'
        )
    parser.add_argument(
        '--url', nargs=1, dest='url', required=True,
        type=str,
        help="""The hostname or IP of the SolrCloud cluster, e.g. solrcloud:8983/solr"""
        )
    parser.add_argument(
        '--collection', nargs=1, dest='collection', required=True,
        type=str,
        help="""Collection name"""
        )
    parser.add_argument(
        '--shard', nargs=1, dest='shard', required=True,
        type=str,
        help="""Shard name"""
        )
    parser.add_argument(
        '--destination_node', nargs=1, dest='destination_node', required=False,
        type=str,
        help="""Optional node to create replica on"""
        )
    parser.add_argument(
        '--request_id', nargs=1, dest='request_id', required=False,
        type=str,
        help="""Request ID to use, the script will monitor for completion of the ID"""
        )
    parser.add_argument(
        '--dry_run', action='store_true', required=False,
        help="""Goes through all the steps except adding the replica"""
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
        configure_logging(logger_name='add_replica', log_level=logging.DEBUG)
    else:
        configure_logging(logger_name='add_replica', log_level=logging.INFO)
    solr_cloud = SolrCloudAdmin(url=args.url[0], log_level=logging.INFO)

    collection = args.collection[0]
    shard = args.shard[0]

    if 'destination_node' in vars(args) and args.destination_node:
        destination_node = args.destination_node[0]
    else:
        destination_node = None

    if 'request_id' in vars(args) and args.request_id:
        request_id = args.request_id[0]
    else:
        request_id = None

    if args.dry_run:
        dryrun = True
    else:
        dryrun = False

    # Check if the live destination node has the source shard
    LOGGER.debug('Checking destination_node for replica')
    if check_node_live(
            solr_cloud=solr_cloud,
            collection=collection,
            shard=shard,
            node_to_check=destination_node
        ):
        LOGGER.warn(
            'Live state.json check lists a replica for \
collection=%s, shard=%s on the destination_node=%s',
            collection,
            shard,
            destination_node
            )
        # return False

    LOGGER.info(
        'Adding replica for collection=%s, shard=%s, destination_node=%s',
        collection,
        shard,
        destination_node
        )

    if dryrun:
        return True

    response = solr_cloud.add_replica(
        collection=collection,
        shard=shard,
        node=destination_node,
        async=request_id
        )
    LOGGER.debug('response=%s', response)
    if response['responseHeader']['status'] != 0 or 'error' in response:
        LOGGER.error(
            'Unable to add replica for collection=%s, \
shard=%s, destination_node=%s. response=%s',
            collection,
            shard,
            destination_node,
            response
            )
        return False
    if request_id is not None and wait_for_async(solr_cloud, request_id):
        LOGGER.info(
            'Successfully added replica for \
collection=%s, shard=%s, destination_node=%s',
            collection,
            shard,
            destination_node
            )
        return True
    elif request_id is None:
        LOGGER.info(
            'Successfully added replica for \
collection=%s, shard=%s, destination_node=%s',
            collection,
            shard,
            destination_node
            )
    else:
        LOGGER.critical(
            'Unable to add replica, check response from wait_for_async.'
            )
        return False

if __name__ == '__main__':
    main()