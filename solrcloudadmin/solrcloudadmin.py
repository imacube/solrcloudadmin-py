"""
Library to handel routine SolrCloud Administration.
"""

import logging
import json
import urllib2

class SolrCloudAdmin(object):
    """
    Handles routine SolrCloud Administration.
    """

    def __init__(self, url=None, loglevel=logging.INFO):
        """
        Set the remote url to use for connections.
        """
        logging.basicConfig(level=loglevel)
        if not url.startswith('http://') and not url.startswith('https://'):
            url = 'http://' + url

        if url.endswith('/'):
            self.url = url[:-1]
        else:
            self.url = url

        logging.debug('self.url: %s', self.url)

    def parse_live_node_title(self, title):
        """
        Returns dictionary with live_node title data.

        Keys: host, port, path
        """
        title_parts = dict()

        split_title = title.split('_') # Split off path
        title_parts['path'] = split_title[1]

        split_title = split_title[0].split(':') # Split off port
        title_parts['port'] = split_title[1]
        title_parts['host'] = split_title[0]

        title_parts['url'] = 'http://%s:%s/%s' % (
            title_parts['host'], title_parts['port'], title_parts['path']
            )

        logging.debug('host: %s', title_parts['host'])
        logging.debug('port: %s', title_parts['port'])
        logging.debug('path: %s', title_parts['path'])
        logging.debug('url: %s', title_parts['url'])

        return title_parts

    def _query(self, path):
        """
        Run query against SolrCloud system.

        Returns JSON response as a dictionary.
        """
        query_url = self._build_url(path)

        logging.debug('query_url: %s', query_url)

        try:
            return json.load(urllib2.urlopen(query_url))
        except urllib2.HTTPError as exception:
            logging.debug('Error returned when opening URL')
            code = exception.code
            reason = json.loads(exception.read())
            logging.debug('code: %d', code)
            logging.debug('reason: %s', reason)
            return reason

    def _build_url(self, path):
        """
        Build URL with path for queries.
        """
        logging.debug('path: %s', path)

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

        live_nodes = list()
        for node in response['tree'][0]['children']:
            live_nodes.append(self.parse_live_node_title(node['data']['title']))

        return live_nodes

    def get_collection_sate(self, collection):
        """
        Return the state for the requested collection.

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
        logging.debug('\n%s', json.dumps(data, sort_keys=True, indent=4))
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
            logging.debug(collection_item)
            collections_list[collection_item] = self.get_collection_sate(collection_item)

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
        logging.debug('collection_dict:\n%s', self.pretty_format(collection_dict))

        return_dict = dict()
        for shard in collection_dict['shards']:
            shard_dict = dict()
            for replica in collection_dict['shards'][shard]['replicas']:
                core_dict = dict()
                core_dict['core'] = collection_dict['shards'][shard]['replicas'][replica]['core']
                core_dict['node_name'] = collection_dict['shards'][shard]['replicas'][replica]['node_name']
                core_dict['state'] = collection_dict['shards'][shard]['replicas'][replica]['state']
                core_dict['base_url'] = collection_dict['shards'][shard]['replicas'][replica]['base_url']
                shard_dict[replica] = core_dict
            return_dict[shard] = shard_dict
        return return_dict

    def cluster_summary(self):
        """
        Build and return a summary of the distribution of collections.
        """
        node_list = dict()
        collection_list = self.list_collection_data()
        for collection in collection_list:
            summary = self.collection_summary(collection_list[collection])
            for shard in summary:
                for core in summary[shard]:
                    node_name = summary[shard][core]['node_name']
                    if node_name in node_list:
                        node_list[node_name] += 1
                    else:
                        node_list[node_name] = 1
        return node_list

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
            path = '%s&async=&s' % (path, str(async))

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

    def check_request_status(self, requestid):
        """
        Check request status and return the result.
        """
        base_path = """/admin/collections?action=REQUESTSTATUS&wt=json&requestid="""

        response = self._query('%s%d' % (base_path, requestid))
        if response['responseHeader']['status'] == 0:
            return response['status']
        else:
            return response

    def move_replica(self, collection, shard, source_node, destination_node):
        """
        Move a replica from one node to another.
        """
        replica = None

        # Get replica to be deleted
        data = self.get_collection_sate(collection=collection)
        for rep in data['shards'][shard]['replicas']:
            if data['shards'][shard]['replicas'][rep]['node_name'] == source_node:
                replica = rep
                break
        if not replica:
            logging.critical('Unable to find replica to be deleted!')
            return {'status': 'failure', 'message': 'Unable to find replica to be deleted'}

        # Add new replica
        response = self.add_replica(collection=collection, shard=shard, node=destination_node)
        if response['responseHeader']['status'] != 0:
            logging.critical(self.pretty_format(response))
            return {'status': 'failure', 'message': 'Status code not 0 for add_replica, see response key.', 'response': response}
        logging.debug(self.pretty_format(response))

        # Delete old replica
        response = self.delete_replica(collection=collection, shard=shard, replica=replica)
        if response['responseHeader']['status'] != 0:
            self.pretty_print(response)
            return {'status': 'failure', 'message': 'Status code not 0 for delete_replica, see response key.', 'response': response}
        logging.debug(self.pretty_format(response))

        return {'status': 'success'}
