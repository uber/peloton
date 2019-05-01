from __future__ import absolute_import

import base64
import json
import logging
import os

from kazoo.client import KazooClient
from thrift.protocol.TJSONProtocol import TJSONProtocol
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.THttpClient import THttpClient
from aurora.api.AuroraSchedulerManager import Client

log = logging.getLogger(__name__)

MEMBER_PREFIX = "member_"
DEFAULT_AURORA_PORT = 8081


class AuroraClient(object):

    """
    Client for talking to the Aurora scheduler

    Sample code on how to run this:

    def foo():
        c = AuroraClient.create('localhost', 8081)
        r = c.getJobs('www-data')
        r2 = c.getJobSummary('www-data')
        print r
        print r2

    """

    @classmethod
    def create(cls, host, port=DEFAULT_AURORA_PORT, encoding="json"):
        """
           Parses a host name and sets up Torando and Thrift client
        """
        # To recieve and send binary thrift to the Aurora master we need to set
        # headers appropriately.
        transport = THttpClient("http://%s:%s/api" % (host, str(port)))

        # Set aurora credentials in transport header if the environment
        # variables have been set
        if os.getenv("AURORA_USERNAME") and os.getenv("AURORA_PASSWORD"):
            username = os.getenv("AURORA_USERNAME")
            password = os.getenv("AURORA_PASSWORD")
            credentials = base64.encodestring(
                "%s:%s" % (username, password)
            ).replace("\n", "")
            auth_header = "Basic %s" % credentials
            transport.setCustomHeaders({"Authorization": auth_header})

        if encoding == "binary":
            transport.setCustomHeaders(
                {
                    "Content-Type": "application/vnd.apache.thrift.binary",
                    "Accept": "application/vnd.apache.thrift.binary",
                }
            )
            protocol = TBinaryProtocol(transport)
        elif encoding == "json":
            protocol = TJSONProtocol(transport)
        else:
            raise Exception("Unknown encoding %s" % encoding)

        client = Client(protocol)
        return client


class AuroraClientZK(object):
    """
    Client for talking to the Aurora scheduler by resolving the current
    Aurora master from a ZK cluster.

    Sample code on how to run this:

    def foo():
        c = AuroraClientZK.create(
            '<zookeeper-url>',
            2181
        )
        r = c.getJobs('www-data')
        r2 = c.getJobSummary('www-data')
        print r
        print r2


    """

    @classmethod
    def create(
        cls, zk_endpoints, encoding="json", zk_path="/aurora/scheduler"
    ):
        """
           Parses a host name and port to a ZK cluster, resolve the current
           Aurora master, and sets up Thrift client for Aurora scheduler.

           The service endpoint is stored in Zookeeper as the following format:

            min@compute31-sjc1:~$ zkcli -h compute36-sjc1 ls /aurora/scheduler
            [u'singleton_candidate_0000000053',
             u'singleton_candidate_0000000054',
             u'member_0000000058',
             u'singleton_candidate_0000000056',
             u'singleton_candidate_0000000057',
             u'singleton_candidate_0000000059']

            min@compute31-sjc1:~$ zkcli -h compute36-sjc1 get \
                /aurora/scheduler/member_0000000058|awk -F\' '{print $2}'|jq .
            {
              "serviceEndpoint": {
                "host": "10.162.9.54",
                "port": 8082
              },
              "additionalEndpoints": {
                "http": {
                  "host": "10.162.9.54",
                  "port": 8082
                }
              },
              "status": "ALIVE"
            }

           TODO: Use @async.context here to create AuroraClient asynchronously

        """

        # Resolve the Aurora master from Zookeeper
        zk_client = KazooClient(",".join(zk_endpoints))
        leader_node_name = None
        try:
            zk_client.start()
            for znode_name in zk_client.get_children(zk_path):
                if znode_name.startswith(MEMBER_PREFIX):
                    leader_node_name = znode_name

            if not leader_node_name:
                raise AuroraClientZKError(
                    "leader name is not defined %s" % zk_endpoints
                )

            leader_node_info = zk_client.get(
                "%s/%s" % (zk_path, leader_node_name)
            )
            instance = json.loads(leader_node_info[0])
            additional_endpoints = instance.get("additionalEndpoints", [])
            for proto in ["https", "http"]:
                if proto in additional_endpoints:
                    endpoint = additional_endpoints[proto]
                    host = endpoint.get("host", None)
                    port = endpoint.get("port", None)
                    if host and port:
                        return AuroraClient.create(host, port, encoding)

            raise AuroraClientZKError(
                "Failed to resolve Aurora endpoint from %s" % zk_endpoints
            )
        finally:
            zk_client.stop()


class AuroraClientZKError(Exception):
    """
    Exception for resolving AuroraClient from Zookeeper cluster.
    """

    pass
