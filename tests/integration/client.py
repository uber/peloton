import os
from urlparse import urlparse

import tools.minicluster.minicluster as minicluster
from peloton_client.client import PelotonClient
from peloton_client.pbgen.peloton.private.resmgrsvc import (
    resmgrsvc_pb2_grpc as resmgr_grpc,
)
from peloton_client.pbgen.peloton.private.hostmgr.hostsvc import (
    hostsvc_pb2_grpc as hostmgr_grpc,
)
from peloton_client.pbgen.peloton.private.hostmgr.v1alpha.svc import (
    hostmgr_svc_pb2_grpc as v1hostmgr_grpc,
)
from util import load_config


class Client(object):
    _client = None

    def __new__(class_, *args, **kwargs):
        if minicluster.default_cluster is not None:
            return minicluster.default_cluster.peloton_client()

        global _client
        if class_._client:
            return class_._client

        config = load_config("config.yaml")["client"]
        cluster = os.getenv("CLUSTER")
        use_apiserver = os.getenv("USE_APISERVER") == 'True'
        if cluster is None or cluster == "local":
            # TODO: remove url overrides once T839783 is resolved
            _client = PelotonClient(
                name=config["name"],
                enable_apiserver=use_apiserver,
                api_url=config["apiserver_url"],
                jm_url=config["jobmgr_url"],
                rm_url=config["resmgr_url"],
                hm_url=config["hostmgr_url"],
            )
            return _client

        if os.getenv("ELECTION_ZK_SERVERS", ""):
            zk_servers = os.getenv("ELECTION_ZK_SERVERS").split(":")[0]
        elif cluster in config["cluster_zk_servers"]:
            zk_servers = config["cluster_zk_servers"][cluster]
        else:
            raise Exception("Unsupported cluster %s" % cluster)

        _client = PelotonClient(
            name=config["name"],
            enable_apiserver=use_apiserver,
            zk_servers=zk_servers,
        )
        return _client


# Decorator which adds private API stubs to the client by monkey patching
# Only implements the resource manager service as of yet.
def with_private_stubs(client):
    rm_loc = urlparse(client.rm_url).netloc
    rm_channel = PelotonClient.resmgr_channel_pool[rm_loc]
    client.resmgr_svc = resmgr_grpc.ResourceManagerServiceStub(
        channel=rm_channel)

    hm_loc = urlparse(client.hm_url).netloc
    hm_channel = PelotonClient.hostmgr_channel_pool[hm_loc]
    client.hostmgr_svc = hostmgr_grpc.InternalHostServiceStub(
        channel=hm_channel)
    client.v1hostmgr_svc = v1hostmgr_grpc.HostManagerServiceStub(
        channel=hm_channel)

    return client
