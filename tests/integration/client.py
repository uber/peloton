import os

from peloton_client.client import PelotonClient
from util import load_config


class Client(object):
    _client = None

    def __new__(class_, *args, **kwargs):
        if not class_._client:
            config = load_config('config.yaml')['client']
            if os.getenv('CLUSTER', ''):
                cluster = os.getenv('CLUSTER')
                if os.getenv('ELECTION_ZK_SERVERS', ''):
                    zk_servers = os.getenv('ELECTION_ZK_SERVERS').split(":")[0]
                elif cluster in config['cluster_zk_servers']:
                    zk_servers = config['cluster_zk_servers'][cluster]
                else:
                    raise Exception('Unsupported cluster %s' % cluster)
                _client = PelotonClient(
                    name=config['name'],
                    zk_servers=zk_servers,
                )
            else:
                # TODO: remove url overrides once T839783 is resolved
                _client = PelotonClient(
                    name=config['name'],
                    jm_url=config['jobmgr_url'],
                    rm_url=config['resmgr_url'],
                )
        return _client
