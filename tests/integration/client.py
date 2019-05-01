import os
from urlparse import urlparse

from peloton_client.client import PelotonClient
from peloton_client.pbgen.peloton.private.resmgrsvc import (
    resmgrsvc_pb2_grpc as resmgr_grpc,
)

from util import load_config


class Client(object):
    _client = None

    def __new__(class_, *args, **kwargs):
        global _client
        if not class_._client:
            config = load_config("config.yaml")["client"]
            cluster = os.getenv("CLUSTER")
            if cluster is not None and cluster != "local":
                cluster = os.getenv("CLUSTER")
                if os.getenv("ELECTION_ZK_SERVERS", ""):
                    zk_servers = os.getenv("ELECTION_ZK_SERVERS").split(":")[0]
                elif cluster in config["cluster_zk_servers"]:
                    zk_servers = config["cluster_zk_servers"][cluster]
                else:
                    raise Exception("Unsupported cluster %s" % cluster)
                _client = PelotonClient(
                    name=config["name"], zk_servers=zk_servers
                )
            else:
                # TODO: remove url overrides once T839783 is resolved
                _client = PelotonClient(
                    name=config["name"],
                    jm_url=config["jobmgr_url"],
                    rm_url=config["resmgr_url"],
                    hm_url=config["hostmgr_url"],
                )
        return _client


# Decorator which adds private API stubs to the client by monkey patching
# Only implements the resource manager service as of yet.
def with_private_stubs(client):
    rm_loc = urlparse(client.rm_url).netloc
    channel = PelotonClient.resmgr_channel_pool[rm_loc]
    client.resmgr_svc = resmgr_grpc.ResourceManagerServiceStub(channel=channel)
    return client
