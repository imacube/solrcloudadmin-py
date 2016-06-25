"""
Queries Zookeeper to get the state's of all the collections
"""

import json
from kazoo.client import KazooClient

# zk = KazooClient(hosts='localhost:2181')
zk = KazooClient(hosts='10.33.16.235:2181')
zk.start() # Open connection to Zookeeper

try:
    for collection in zk.get_children('/collections/'):
        state = json.loads(zk.get('/collections/%s/state.json' % collection)[0])
        # print json.dumps(state[collection]['shards'], indent=2)
        for shard in state[collection]['shards'].values():
            # print json.dumps(shard['replicas'], indent=2)
            for replica in shard['replicas'].values():
                print '%s %s' % (replica['core'],  replica['state'])
        # break
except Exception as e:
    print e
finally:
    zk.stop()
