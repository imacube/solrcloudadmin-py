"""
Library to handel routine SolrCloud Administration.
"""

import logging
import json
import urllib2
import time

class SolrCloudAdmin(object):
    """
    Handles routine SolrCloud Administration.
    """

    def __init__(self, url=None, log_level=logging.INFO, max_retries=3, retry_sleeptime=10):
        """
        Set the remote url to use for connections.
        :arg url: URL to use for connections
        :arg log_level: log level to set for all logging
        :arg max_retries: Number of times to retry running a query against
            SolrCloud cluster
        :arg retry_sleeptime: How long in seconds to sleep between query retries
        """
        logger = logging.getLogger(__name__)
        logger.setLevel(log_level)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s.%(funcName)s, line %(lineno)d - \
%(levelname)s - %(message)s'
            )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        self.logger = logger
        self.logger.debug('Starting init of %s', __name__)

        self.set_url(url)

        self.logger.debug('self.url: %s', self.url)

        self.max_retries = max_retries
        self.logger.debug('max_retries = %d', max_retries)

        self.retry_sleeptime = retry_sleeptime
        self.logger.debug('retry_sleeptime = %d', retry_sleeptime)

    def set_url(self, url):
        """
        Set the URL to be used by queries
        """
        if type(url) == list:
            url = url[0]
        if not url.startswith('http://') and not url.startswith('https://'):
            url = 'http://' + url

        if url.endswith('/'):
            self.url = url[:-1]
        else:
            self.url = url

    def parse_live_node_title(self, title):
        """
        Returns dictionary with live_node title data.

        Keys: host, port, path
        """
        title_parts = {'node_name': title}

        split_title = title.split('_') # Split off path
        title_parts['path'] = split_title[1]

        split_title = split_title[0].split(':') # Split off port
        title_parts['port'] = split_title[1]
        title_parts['host'] = split_title[0]

        title_parts['url'] = 'http://%s:%s/%s' % (
            title_parts['host'], title_parts['port'], title_parts['path']
            )

        self.logger.debug('node_name: %s', title)
        self.logger.debug('host: %s', title_parts['host'])
        self.logger.debug('port: %s', title_parts['port'])
        self.logger.debug('path: %s', title_parts['path'])
        self.logger.debug('url: %s', title_parts['url'])

        return title_parts

    def _query(self, path):
        """
        Run query against SolrCloud system.

        Returns JSON response as a dictionary.
        """
        query_url = self._build_url(path)

        self.logger.debug('query_url: %s', query_url)

        count = 0
        while count <= self.max_retries:
            count += 1
            self.logger.debug('Try %d', count)

            try:
                request = urllib2.urlopen(query_url)
                data = request.read()
                json_data = json.loads(data)
                self.logger.debug('request data: %s', data)
                self.logger.debug('json: %s: ', json_data)
                return json_data
            except urllib2.HTTPError as exception:
                self.logger.debug('Error returned when opening URL')
                code = exception.code
                reason = exception.read()
                self.logger.error('code: %d', code)
                self.logger.error('reason: %s', reason)
            except ValueError as exception:
                self.logger.critical('ValueError exception thrown when decoding JSON')
                self.logger.critical('Raw data returned:\n%s', data)
                return None
            if count <= self.max_retries:
                time.sleep(self.retry_sleeptime)

    def _build_url(self, path):
        """
        Build URL with path for queries.
        """
        self.logger.debug('path: %s', path)

        return self.url + path

    def pretty_format(self, data, sort_keys=True, indent=4):
        """
        Function to pretty format JSON or dictionary items.
        """
        return json.dumps(data, sort_keys=sort_keys, indent=indent)

    def pretty_print(self, data, sort_keys=True, indent=4):
        """
        Function to pretty print JSON or dictionary items.
        """
        print self.pretty_format(data, sort_keys, indent)

    def list_live_nodes(self):
        """
        List the live nodes in the SolrCloud cluster.

        /zookeeper?detail=true&path=%2Flive_nodes
        """
        response = self._query('/zookeeper?detail=true&path=%2Flive_nodes')

        live_nodes = dict()
        for node in response['tree'][0]['children']:
            node_dict = self.parse_live_node_title(node['data']['title'])
            live_nodes[node_dict['node_name']] = node_dict

        return live_nodes

    def get_collection_state(self, collection):
        """
        Return the state for the requested collection.

        This includes information on shards, replicas, and other
        important information.

        /zookeeper?detail=true&path=%2Fcollections%2Faws-stage-442%2Fstate.json
        """
        base_path = """/zookeeper?detail=true&path=%2Fcollections"""
        state_json = '%2Fstate.json'

        collection_data = self._query(
            base_path +
            '%2F' +
            collection +
            state_json
        )
        data = json.loads(collection_data['znode']['data'])[collection]
        self.logger.debug('\n%s', json.dumps(data, sort_keys=True, indent=4))
        return data

    def list_collections_only(self):
        """
        Returns a set of collections with no other data.

        /zookeeper?detail=true&path=%2Fcollections
        """
        base_path = """/zookeeper?detail=true&path=%2Fcollections"""

        response = self._query(base_path)

        collections_list = set()
        for collection_item in response['tree'][0]['children']:
            collections_list.add(collection_item['data']['title'])

        return collections_list

    def list_collection_data(self, limit=0):
        """
        Returns a dictionary containing dictionaires for all the collections
        in the cluster.
        """
        collections_list = dict()
        return_count = 0

        for collection_item in self.list_collections_only():
            self.logger.debug(collection_item)
            collections_list[collection_item] = self.get_collection_state(collection_item)

            if limit > 0:
                return_count += 1
            if limit > 0 and return_count >= limit:
                break

        return collections_list

    def collection_summary(self, collection_dict):
        """
        Given a dictionary of collection data returns a summary dictionary
        of the collection's cores.
        """
        self.logger.debug('collection_dict:\n%s', self.pretty_format(collection_dict))

        return_dict = dict()
        for shard in collection_dict['shards']:
            shard_dict = dict()
            for replica in collection_dict['shards'][shard]['replicas']:
                core_dict = dict()
                core_dict['core'] = \
                    collection_dict['shards'][shard]['replicas'][replica]['core']
                core_dict['node_name'] = \
                    collection_dict['shards'][shard]['replicas'][replica]['node_name']
                core_dict['state'] = \
                    collection_dict['shards'][shard]['replicas'][replica]['state']
                core_dict['base_url'] = \
                    collection_dict['shards'][shard]['replicas'][replica]['base_url']
                shard_dict[replica] = core_dict
            return_dict[shard] = shard_dict
        return return_dict

    def collection_count(self):
        """
        This function has been renamed, count_all_cores
        """
        self.logger.warn('This function has been renamed count_all_cores')
        return self.count_all_cores()

    def count_all_cores(self):
        """
        Build and return a summary of the count of cores on all SolrCloud nodes, includes down nodes
        """

        node_list = dict()
        node_state = dict()
        for node_name in self.list_live_nodes():
            node_list[node_name] = 0
            node_state[node_name] = 'live'

        for collection_data in self.list_collection_data().values():
            summary = self.collection_summary(collection_data)
            for shard in summary:
                for core in summary[shard]:
                    node_name = summary[shard][core]['node_name']
                    if node_name in node_list:
                        node_list[node_name] += 1
                    else:
                        node_list[node_name] = 1
                        node_state[node_name] = 'down'
        return {'node_list': node_list, 'node_state': node_state}

    def count_live_cores(self):
        """
        Return count of live cores, only works for live nodes
        """
        orig_url = self.url # Store URL to be reset at end

        core_count_dict = {'node_list': dict()}

        for key in self.list_live_nodes().values():
            url = key['url']
            node_name = key['node_name']

            self.logger.debug('Setting url=%s', url)
            self.set_url(url)

            node_core_info = self.get_core_status()
            core_count = len(node_core_info['status'].keys())
            self.logger.debug(
                'node_name=%s, core_count=%d',
                node_name,
                core_count
                )

            core_count_dict['node_list'][node_name] = core_count

        self.set_url(orig_url)
        return core_count_dict

    def get_core_status(self, core=None):
        """
        Get information for the requested core.
        /admin/cores?action=STATUS&wt=json&core=<core to lookup>
        """
        base_path = """/admin/cores?action=STATUS&wt=json"""

        if core:
            response = self._query('%s&core=%s' % (base_path, core))
        else:
            response = self._query('%s' % (base_path))
        return response

    def add_replica(self, collection, shard, node=None, async=None):
        """
        Add a replica of a collections shard.

        /admin/collections?action=ADDREPLICA&collection=<collection>\
        &shard=<shard>&node=<optional: solr node name>&async=<optioanl: async request id>
        """
        base_path = """/admin/collections?action=ADDREPLICA&wt=json"""
        path = '%s&collection=%s' % (base_path, collection)
        path = '%s&shard=%s' % (path, shard)
        if node:
            path = '%s&node=%s' % (path, node)
        if async:
            path = '%s&async=%s' % (path, str(async))

        self.logger.debug('path: %s', path)
        response = self._query(path)

        return response

    def delete_replica(self, collection, shard, replica, only_if_down=None):
        """
        Delete a replica of a collections shard.

        /admin/collections?action=DELETEREPLICA\
        &collection=collection&shard=shard&replica=replica
        """
        base_path = """/admin/collections?action=DELETEREPLICA&wt=json"""
        path = '%s&collection=%s' % (base_path, collection)
        path = '%s&shard=%s' % (path, shard)
        path = '%s&replica=%s' % (path, replica)
        if only_if_down:
            path = '%s&onlyIfDown=%s' % (path, only_if_down)

        response = self._query(path)

        return response

    def get_request_status(self, requestid):
        """
        Check request status and return the result.
        """
        base_path = """/admin/collections?action=REQUESTSTATUS&wt=json&requestid="""

        response = self._query('%s%s' % (base_path, str(requestid)))
        self.logger.debug('response = %s', response)
        return response

    def move_shard(self, collection, shard, source_node, destination_node=None, async=None):
        """
        Move a replica of a shard from one node to another.
        """
        replica = None

        # Get replica to be deleted
        data = self.get_collection_state(collection=collection)
        for rep in data['shards'][shard]['replicas']:
            if data['shards'][shard]['replicas'][rep]['node_name'] == source_node:
                replica = rep
                break
        if not replica:
            self.logger.critical('Unable to find replica to be deleted!')
            return {'status': 'failure', 'message': 'Unable to find replica to be deleted'}

        return self.move_replica(
            collection=collection,
            shard=shard,
            replica=replica,
            destination_node=destination_node,
            async=async
            )

    def move_replica(self, collection, shard, replica, destination_node=None, async=None):
        """
        Move a specific replica from one node to another.
        """
        # Add new replica
        response = self.add_replica(
            collection=collection,
            shard=shard,
            node=destination_node,
            async=async
            )
        if response['responseHeader']['status'] != 0:
            self.logger.critical(self.pretty_format(response))
            return {
                'status': 1,
                'message': 'Status code not 0 for add_replica, see response key.',
                'response': response
                }
        self.logger.debug(self.pretty_format(response))

        # Wait for job to finish
        while True:
            response = self.get_request_status(async)
            if response['status'] != 0:
                self.logger.critical(
                    'Failed to check status of async request ID %s\n%s',
                    str(async),
                    self.pretty_format(response)
                    )
                return {
                    'status': 1,
                    'message': 'Unexpected request status result, see response key.',
                    'response': response
                }
            elif response['response']['status']['state'] == 'running':
                self.logger.debug('Waiting for job to finish...')
                time.sleep(2)
                continue
            elif response['response']['status']['state'] == 'completed':
                break
            else:
                self.logger.critical('Unexpected task state\n%s', self.pretty_format(response))
                return {
                    'status': 1,
                    'message': 'Unexpected request status result, see response key.',
                    'response': response
                }

        # Delete old replica
        response = self.delete_replica(collection=collection, shard=shard, replica=replica)
        if response['responseHeader']['status'] != 0:
            self.pretty_print(response)
            return {
                'status': 1,
                'message': 'Status code not 0 for delete_replica, see response key.',
                'response': response
                }
        self.logger.debug(self.pretty_format(response))

        return {
            'status': 0,
            'message': 'See response key',
            'response': response
            }
