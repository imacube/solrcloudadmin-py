"""
Library to handel routine SolrCloud Administration.
"""

from json.decoder import JSONDecodeError
import json
import logging

from kazoo.client import KazooClient
import requests

class SolrCloudCollectionsApi(object):
    """
    Call SolrCloud Collections API commands
    """

    def __init__(self, solr_cloud_url=None, zookeeper_urls=None, log_level=logging.INFO, timeout=5, max_retries=3, retry_sleeptime=10):
        """
        Set the remote url to use for connections.
        :arg solr_cloud: SolrCloud IP or hostname ane port
        :arg log_level: log level to set for all logging
        :arg timeout: if number then howlong to wait for connection or data, if tuple then timeouts for (connect, read); set to None to wait forever
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

        self.solr_cloud_url = self.prepare_solr_cloud_url(solr_cloud_url)
        self.logger.debug('solr_cloud_url = %s' % solr_cloud_url)
        self.set_zookeeper_urls(zookeeper_urls)

        self.timeout = timeout
        self.logger.debug('timeout = %d' % timeout)

        self.max_retries = max_retries
        self.logger.debug('max_retries = %d' % max_retries)

        self.retry_sleeptime = retry_sleeptime
        self.logger.debug('retry_sleeptime = %d' % retry_sleeptime)

    def set_zookeeper_urls(self, zookeeper_urls):
        """
        Set the URL of one or more Zookeeper instances
        """
        self.zookeeper_urls = zookeeper_urls
        self.logger.debug('zookeeper_urls = %s' % zookeeper_urls)

    def prepare_solr_cloud_url(self, solr_cloud_url):
        """
        Set the URL of the SolrCloud cluster
        """
        if not solr_cloud_url.startswith(('http://', 'https://')):
            solr_cloud_url = 'http://' + solr_cloud_url

        if solr_cloud_url.endswith('/'):
            return solr_cloud_url[:-1]
        else:
            return solr_cloud_url

    def make_get_request(self, path, parameters=None, solr_cloud_url=None):
        """
        Make a get request to SolrCloud
        """
        if not solr_cloud_url:
            solr_cloud_url = self.solr_cloud_url
        else:
            solr_cloud_url = self.prepare_solr_cloud_url(solr_cloud_url)
        self.logger.debug('Using solr_cloud_url = %s' % (solr_cloud_url))
        return requests.get('%s%s' % (solr_cloud_url, path), params=parameters, timeout=self.timeout)

    def build_request_path(self, action, base_path='/solr/admin/collections', api='collections', json_format=True, **kwargs):
        """
        Build the URL request path that will give a JSON formated response
        """
        if api == 'collections':
            base_path = '/solr/admin/collections'
        elif api == 'cores':
            base_path = '/solr/admin/cores'

        path = '%s?action=%s' % (base_path, action)
        parameters = dict()
        for key, value in kwargs.items():
            if value:
                parameters[key] = value
        if json_format:
            parameters['wt'] = 'json'
        return path, parameters

    def add_replica(self, collection, shard='shard1', node=None):
        """
        Add a replica to a collection
        /admin/collections?action=ADDREPLICA&collection=collection&shard=shard&node=solr_node_name
        """
        action = 'ADDREPLICA'
        path, parameters = self.build_request_path(action=action, collection=collection, shard=shard, node=node)
        return self.make_get_request(path=path, parameters=parameters)

    def delete_replica(self, collection, shard, replica, async=None):
        """
        Delete a collections replica
        /admin/collections?action=DELETEREPLICA&collection=collection&shard=shard&replica=replica
        """
        action = 'DELETEREPLICA'
        path, parameters = self.build_request_path(action=action, collection=collection, shard=shard, replica=replica, sync=async)
        return self.make_get_request(path=path, parameters=parameters)

    def solr_cloud_list_collections(self):
        """
        List all collections by querying SolrCloud
        /admin/collections?action=LIST
        """
        action = 'LIST'
        path, parameters = self.build_request_path(action=action)
        return self.make_get_request(path, parameters)

    def get_core_status(self, node_name=None, core=None):
        """
        Get information for all cores on a SolrCloud node or
        for a the specified core.
        /admin/cores?action=STATUS&wt=json&core=<core to lookup>
        """
        action = 'STATUS'

        # Parse node name, also works if IP/HOSTNAME and Port
        # 10.20.30.40:8983_solr
        solr_cloud_url = None
        if node_name:
            solr_cloud_url = node_name.split('_')[0:1][0]

        path, parameters = self.build_request_path(action=action, api='cores', core=core)
        return self.make_get_request(path, parameters, solr_cloud_url=solr_cloud_url)

    def zookeeper_list_collections(self, zookeeper_urls=None):
        """
        List all collections by querying Zookeeper
        """
        if not zookeeper_urls:
            zookeeper_urls = self.zookeeper_urls
        zk = KazooClient(hosts=zookeeper_urls)
        zk.start()

        try:
            return zk.get_children('/collections/')
        except Exception as e:
            self.logger.critical(e)
        finally:
            zk.stop()

    def parse_zookeeper_response(self, data):
        """
        Parse out the Zookeeper response from zk.get requests
        """
        return (json.loads(data[0].decode('utf-8')), data[1:])

    def get_collection_state(self, collection, zookeeper_urls=None):
        """
        Get the collection's state json file from zookeeper
        """
        if not zookeeper_urls:
            zookeeper_urls = self.zookeeper_urls
        zk = KazooClient(hosts=zookeeper_urls)
        zk.start() # Open connection to Zookeeper

        try:
            self.logger.debug('Get collection state for collection %s' % collection)
            response = zk.get('/collections/%s/state.json' % collection)
            self.logger.debug('response=%s' % str(response))
            return self.parse_zookeeper_response(list(response))
        except Exception as e:
            self.logger.critical(e)
            raise e
        finally:
            zk.stop()

    def get_live_solrcloud_nodes(self, zookeeper_urls=None):
        """
        Get a list of SolrCloud nodes
        """
        if not zookeeper_urls:
            zookeeper_urls = self.zookeeper_urls
        zk = KazooClient(hosts=zookeeper_urls)
        zk.start() # Open connection to Zookeeper

        try:
            solrcloud_nodes = set()
            for node in zk.get_children('/live_nodes'):
                solrcloud_nodes.add(node)

            return solrcloud_nodes
        except Exception as e:
            self.logger.critical(e)
            raise e
        finally:
            zk.stop()

    def parse_collection_name(self, core_name):
        """
        Parse out a collections name from its core name

        Format: {collection name}_{shard}_{replica}
        Reverse the string and trip off the first to parts using an _ seperator, then
        rebuild the remainder part in proper order. Assumes that replica and shard names don't
        have _ characters in them.
        """
        return str().join(map(str, core_name[::-1].split('_')[2:]))[::-1]
