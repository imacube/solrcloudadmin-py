"""
Queries Zookeeper to get the state's of all the collections
"""

import json
import urllib2
from time import sleep
from kazoo.client import KazooClient

zk = KazooClient(hosts='10.33.16.235:2181')
zk.start() # Open connection to Zookeeper

def query(query_url):
    """
    Run query against SolrCloud system.

    Returns JSON response as a dictionary.
    """
    try:
        request = urllib2.urlopen(query_url)
        data = request.read()
        json_data = json.loads(data)
        return json_data
    except Exception as exception:
        print exception
        return None

document_count = 0

try:
    for collection in zk.get_children('/collections/'):
        state = json.loads(zk.get('/collections/%s/state.json' % collection)[0])
        for shard in state[collection]['shards'].values():
            # print json.dumps(shard['replicas'], indent=2)
            for replica in shard['replicas'].values():
                print '%s' % (collection)
                # print replica['core']
                # print '%s %s' % (replica['core'],  replica['state'])
                # print replica['base_url']
                # print collection
                while True:
                    json_data = query("""http://10.33.16.235:8983/solr/""" + replica['core'] + """/select?q=*%3A*&wt=json&indent=true&rows=1""")
                    if json_data:
                        break
                    else:
                        print "sleeping for 30 seconds"
                        sleep(30)
                document_count += json_data['response']['numFound']
                break
        # break
except Exception as e:
    print e
finally:
    zk.stop()
    print document_count
