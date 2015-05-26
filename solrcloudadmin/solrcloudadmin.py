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

        return json.load(urllib2.urlopen(query_url))

    def _build_url(self, path):
        """
        Build URL with path for queries.
        """
        logging.debug('path: %s', path)

        return self.url + path

    def pretty_print(self, data, sort_keys=True, indent=4):
        """
        Function to pretty print json as needed
        """
        print json.dumps(data, sort_keys=sort_keys, indent=indent)

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

    def list_collections(self):
        """
        Returns a dictionary containing dictionaires for all the collections
        in the cluster

        /zookeeper?detail=true&path=%2Fcollections
        /zookeeper?detail=true&path=%2Fcollections%2Faws-stage-442%2Fstate.json
        """
        base_path = """/zookeeper?detail=true&path=%2Fcollections"""
        state_json = '%2Fstate.json'
        response = self._query(base_path)

        collections_list = dict()
        for collection_item in response['tree'][0]['children']:
            logging.debug(collection_item['data']['title'])
            collection_data = self._query(
                base_path +
                '%2F' +
                collection_item['data']['title'] +
                state_json
            )
            collections_list[collection_item['data']['title']] = json.loads(collection_data['znode']['data'])

        return collections_list
