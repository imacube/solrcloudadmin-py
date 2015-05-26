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

    def __init__(self, host=None, loglevel=logging.INFO):
        """
        Set the remote host to use for connections.
        """
        logging.basicConfig(level=loglevel)
        if not host.startswith('http://') and not host.startswith('https://'):
            host = 'http://' + host

        if host.endswith('/'):
            self.host = host[:-1]
        else:
            self.host = host

        logging.debug('self.host: %s', self.host)

    def _query(self, path):
        """
        Run query against SolrCloud system.

        Returns JSON response as a dictionary.
        """
        url = self._build_url(path)

        logging.debug('url: %s', url)

        return json.load(urllib2.urlopen(url))

    def _build_url(self, path):
        """
        Build URL with path for queries.
        """
        logging.debug('path: %s', path)

        url = self.host + path

        return url

    def pretty_print(self, data, sort_keys=True, indent=4):
        """
        Function to pretty print json as needed
        """
        print json.dumps(data, sort_keys=sort_keys, indent=indent)

    def list_live_nodes(self):
        """
        List the live hosts in the SolrCloud cluster.

        /zookeeper?detail=true&path=%2Flive_nodes
        """
        response = self._query('/zookeeper?detail=true&path=%2Flive_nodes')

        live_nodes = dict()
        for node in response['tree'][0]['children']:
            live_nodes[node['data']['title']] = node['data']['title']

        return live_nodes
