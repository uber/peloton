from docker import errors as docker_errors
import json
import logging

from peloton_client.client import PelotonClient

import components
import driver_base
from tools.minicluster import minicluster

log = logging.getLogger(__name__)


class PelotonClientWrapper(PelotonClient):
    """
    Wrapper on the standard Peloton client to patch certain
    parts of the data retrieved from Zookeeper. This is
    needed to workaround the fact that Docker containers
    created by minicluster cannot be directly addressed with
    the information found in Zookeeper.
    """

    def _on_job_mgr_leader_change(self, data, stat, event):
        data = self._patch_leader_ip("job_mgr", data)
        super(PelotonClientWrapper, self)._on_job_mgr_leader_change(
            data, stat, event
        )

    def _on_res_mgr_leader_change(self, data, stat, event):
        data = self._patch_leader_ip("res_mgr", data)
        super(PelotonClientWrapper, self)._on_res_mgr_leader_change(
            data, stat, event
        )

    def _on_host_mgr_leader_change(self, data, stat, event):
        data = self._patch_leader_ip("host_mgr", data)
        super(PelotonClientWrapper, self)._on_host_mgr_leader_change(
            data, stat, event
        )

    def _patch_leader_ip(self, comp_name, data):
        if data.startswith("{"):
            try:
                leader = json.loads(data)
                leader["ip"] = "localhost"
                log.info("Patching %s leader with %s", comp_name, leader)
                data = json.dumps(leader)
            except Exception as e:
                log.warn("Failed to patch leader data: %s", e)
        return data


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

    def __init__(self):
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
        containers = minicluster.cli.containers(all=not running_only)
        ids = {}
        for c in containers:
            for n in c["Names"]:
                if n.startswith("/" + cont_name):
                    ids[c["Id"]] = n
        return ids

    def start(self, cid):
        try:
            minicluster.cli.start(cid)
        except docker_errors.APIError as e:
            if e.response.status_code != self.ERROR_NOT_MODIFIED:
                raise

    def stop(self, cid):
        try:
            minicluster.cli.stop(cid)
        except docker_errors.APIError as e:
            if e.response.status_code != self.ERROR_NOT_MODIFIED:
                raise

    def execute(self, cid, *cmd_and_args):
        ex = minicluster.cli.exec_create(
            cid, " ".join(cmd_and_args), stdout=True, stderr=True
        )
        minicluster.cli.exec_start(ex, tty=True)

    def match_zk_info(self, cid, cinfo, zk_node_info):
        return cinfo == "/" + zk_node_info["hostname"]

    def get_peloton_client(self, name):
        return PelotonClientWrapper(name=name, zk_servers="localhost:8192")

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
