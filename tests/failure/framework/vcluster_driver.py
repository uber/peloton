import os

from peloton_client.client import PelotonClient
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task

import components
import driver_base
from tools.vcluster import vcluster


class VClusterDriver(driver_base.ClusterDriverABC):
    """
    Driver to make failure-testing framework work with a Peloton
    cluster set up on vcluster. Expects the Peloton cluster to
    already exist using the default-small configuration.
    Component instance identifier: Pair of the (Job ID, Task instance ID) as
    defined in Peloton API.
    Component instance information: TaskInfo as defined in Peloton API
    """

    def __init__(self):
        self.component_name_map = {}

    def setup(self, *args, **kwargs):
        """
        Loads information about a deployed vcluster from a file.
        The file to read from must be specified in environment variable
        VCLUSTER_INFO.
        :param args: Not used
        :param kwargs: Not used
        """
        conf_file = os.environ.get("VCLUSTER_INFO")
        if not conf_file:
            raise Exception(
                "Environment variable VCLUSTER_INFO "
                + "not set to vcluster output configuration file"
            )
        self.vcluster = vcluster.vcluster_from_conf(conf_file)

        self.component_name_map.update(
            {
                components.MesosMaster().name: "mesos-master",
                components.MesosAgent().name: "mesos-slave",
                components.Zookeeper().name: "zookeeper",
                components.HostMgr().name: "hostmgr",
                components.JobMgr().name: "jobmgr",
                components.ResMgr().name: "resmgr",
                components.BatchPlacementEngine().name: "placement",
                components.StatelessPlacementEngine().name: "placement_stateless",
            }
        )

    def teardown(self, *args, **kwargs):
        pass

    def find(self, component_name, running_only=True):
        job_name = self._resolve_component_name(component_name)
        ph = self.vcluster.peloton_helper
        job_id = self.vcluster.vcluster_config["job_info"].get(job_name)
        if not job_id:
            return []
        tasks = ph.get_tasks(job_id)
        return {
            (job_id, i): t
            for i, t in tasks.iteritems()
            if not running_only
            or task.TaskState.Name(t.runtime.state) == "RUNNING"
        }

    def start(self, cid):
        self.vcluster.peloton_helper.start_task(cid[0], cid[1])

    def stop(self, cid):
        self.vcluster.peloton_helper.stop_task(cid[0], cid[1])

    def execute(self, cid, *cmd_and_args):
        raise Exception("Not implemented")

    def match_zk_info(self, cid, cinfo, zk_node_info):
        rt = cinfo.runtime
        return (
            rt.host == zk_node_info["hostname"]
            and rt.ports.get("GRPC_PORT") == zk_node_info["grpc"]
        )

    def get_peloton_client(self, name):
        return PelotonClient(
            name=name, zk_servers=self.vcluster.virtual_zookeeper
        )

    def info(self):
        res = [
            ("Base Peloton - zookeeper", self.vcluster.zk_server),
            ("Base Peloton - job label", self.vcluster.label_name),
            ("Zookeeper location", self.vcluster.virtual_zookeeper),
        ]
        for comp_name in self.component_name_map:
            job_name = self._resolve_component_name(comp_name)
            job_id = self.vcluster.vcluster_config["job_info"].get(job_name)
            if job_id:
                res.append((comp_name, "job %s" % job_id))
        return res

    def _resolve_component_name(self, comp_name):
        return self.component_name_map.get(comp_name, comp_name)
