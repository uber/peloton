from docker import errors as docker_errors
import logging

import components
import driver_base
from tools.minicluster import client

log = logging.getLogger(__name__)


class MiniClusterDriver(driver_base.ClusterDriverABC):
    """
    Driver to make failure-testing framework work with a Peloton
    cluster set up on minicluster. Expects the Peloton cluster to
    already exist using the default configuration.
    Component instance identifier: String that contains the Docker
    container ID.
    Component instance information: String that contains the name
    of the Docker container as returned by Docker REST APIs.
    """

    # Docker error to indicate that a start/stop operation was
    # ignored because the container was already started/stopped
    ERROR_NOT_MODIFIED = 304

    def __init__(self, docker_client):
        self.docker_client = docker_client
        self.component_name_map = {
            components.MesosMaster().name: "peloton-mesos-master",
            components.MesosAgent().name: "peloton-mesos-agent",
            components.Zookeeper().name: "peloton-zk",
            components.Cassandra().name: "peloton-cassandra",
            components.HostMgr().name: "peloton-hostmgr",
            components.JobMgr().name: "peloton-jobmgr",
            components.ResMgr().name: "peloton-resmgr",
            components.BatchPlacementEngine().name: "peloton-placement",
            components.StatelessPlacementEngine().name: "peloton-placement",
            components.AuroraBridge().name: "peloton-aurorabridge",
        }

    def setup(self, *args, **kwargs):
        pass

    def teardown(self, *args, **kwargs):
        pass

    def find(self, component_name, running_only=True):
        cont_name = self._resolve_component_name(component_name)
        containers = self.docker_client.containers(all=not running_only)
        ids = {}
        for c in containers:
            for n in c["Names"]:
                if n.startswith("/" + cont_name):
                    ids[c["Id"]] = n
        return ids

    def start(self, cid):
        try:
            self.docker_client.start(cid)
        except docker_errors.APIError as e:
            if e.response.status_code != self.ERROR_NOT_MODIFIED:
                raise

    def stop(self, cid):
        try:
            self.docker_client.stop(cid)
        except docker_errors.APIError as e:
            if e.response.status_code != self.ERROR_NOT_MODIFIED:
                raise

    def execute(self, cid, *cmd_and_args):
        ex = self.docker_client.exec_create(
            cid, " ".join(cmd_and_args), stdout=True, stderr=True
        )
        self.docker_client.exec_start(ex, tty=True)

    def match_zk_info(self, cid, cinfo, zk_node_info):
        return cinfo == "/" + zk_node_info["hostname"]

    def get_peloton_client(self, name):
        return client.PelotonClientWrapper(name=name,
                                           zk_servers="localhost:8192")

    def info(self):
        res = [("Docker containers", "")]
        for comp_name in self.component_name_map:
            cids = self.find(comp_name, running_only=False)
            res.append(
                (
                    comp_name,
                    ", ".join(
                        ["%s (%s)" % (k, v) for k, v in cids.iteritems()]
                    ),
                )
            )
        return res

    def _resolve_component_name(self, comp_name):
        return self.component_name_map.get(comp_name, comp_name)
