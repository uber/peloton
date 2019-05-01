import json
import time
import uuid

from kazoo.client import KazooClient

from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton

from config_generator import create_mesos_task_config

from color_print import print_okblue


RUNNING_TARGET_STATUS = {"RUNNING": (1, float("inf"))}

KILLED_TARGET_STATUS = {"RUNNING": (float("-inf"), 0)}


class ModuleLaunchFailedException(Exception):
    pass


class Module(object):
    def __init__(self, module_name, label_name, config, peloton_helper):
        """
        param module_name: the name of the job
        param label_name: the label of the job
        param config: vcluster config
        param peloton_helper: instance of PelotonClientHelper

        type module_name: str
        type label_name: str
        type config: dict
        type peloton_helper: PelotonClientHelper

        """
        self.name = module_name
        self.label = label_name

        self.config = config
        self.peloton_helper = peloton_helper
        self.job_id = ""
        self.version = ""

    def setup(
        self,
        dynamic_env,
        instance_number,
        job_name=None,
        version=None,
        image_path=None,
    ):
        """
        param dynamic: dict of dynamic environment virable
        param instance_number: number of tasks in the job

        type dynamic: dict
        type instance_number: int

        return: job-id
        """
        if not job_name:
            job_name = self.label + "_" + self.name
        task_config = create_mesos_task_config(
            self.config, self.name, dynamic_env, version, image_path
        )
        if version:
            self.version = version

        resp = self.peloton_helper.create_job(
            label=self.label,
            name=job_name,
            default_task_config=task_config,
            num_instance=instance_number,
        )
        self.job_id = resp.jobId.value
        print_okblue("Waiting for job %s creating..." % job_name)
        if not self.peloton_helper.monitering(
            self.job_id, RUNNING_TARGET_STATUS
        ):
            raise ModuleLaunchFailedException("%s can not launch" % self.name)
        return self.job_id

    def teardown(self, job_name=None, remove=False):
        """
        param job_name: name of the job so specify
        type job_name: str
        """
        if not job_name:
            job_name = self.label + "_" + self.name
        states = [] if remove else ["RUNNING", "PENDING", "INITIALIZED"]
        ids = self.peloton_helper.get_jobs_by_label(
            self.label, job_name, states
        )
        for id in ids:
            self.peloton_helper.stop_job(id)
            self.peloton_helper.monitering(id, KILLED_TARGET_STATUS)
        if remove:
            NOT_IN_KILLING_STATE = {"KILLING": (float("-inf"), 0)}
            for id in ids:
                self.peloton_helper.monitering(id, NOT_IN_KILLING_STATE)
                self.peloton_helper.delete_job(id)

    def get_host_port_for_instance(self, instance_num, port_name):
        """
        rtype: host, port: str, str
        """
        if self.job_id:
            ids = [self.job_id]
        else:
            ids = self.peloton_helper.get_jobs_by_label(
                self.label, self.label + "_" + self.name, ["RUNNING"]
            )
            if len(ids) == 0:
                raise Exception("No jobs found for %s" % self.name)

        tasks = self.peloton_helper.get_tasks(ids[0])
        if len(tasks) == 0:
            raise Exception("No instances found for %s" % self.name)
        if instance_num >= len(tasks):
            raise Exception(
                "Instance %d not found for %s" % (instance_num, self.name)
            )
        host = tasks[instance_num].runtime.host
        port = tasks[instance_num].runtime.ports.get(port_name)
        if not port:
            raise Exception(
                "Port %s not found for %s,%s"
                % (port_name, self.name, instance_num)
            )
        return host, port


class Zookeeper(Module):
    def __init__(self, label_name, config, peloton_helper):
        """
        type param label_name: str
        type config: dict
        type peloton_helper: PelotonClientHelper
        """
        super(Zookeeper, self).__init__(
            "zookeeper", label_name, config, peloton_helper
        )

    def get_host_port(self):
        """
        rtype: host, port: str, str
        """
        return self.get_host_port_for_instance(0, "ZOO_PORT")


class MesosMaster(Module):
    def __init__(self, label_name, config, peloton_helper):
        """
        type param label_name: str
        type config: dict
        type peloton_helper: PelotonClientHelper
        """
        super(MesosMaster, self).__init__(
            "mesos-master", label_name, config, peloton_helper
        )

    def find_leader(self, zk_host):
        """
        :return: a dict of {job_id: instance_index}
        :rtype: dict
        """
        zk = KazooClient(hosts=zk_host, read_only=True)
        zk.start()
        znode, _ = zk.get("/mesos/json.info_0000000001")
        leader = json.loads(znode)

        return leader["hostname"], leader["port"]


class MesosSlave(Module):
    def __init__(self, label_name, config, peloton_helper):
        """
        type param label_name: str
        type config: dict
        type peloton_helper: PelotonClientHelper
        """
        super(MesosSlave, self).__init__(
            "mesos-slave", label_name, config, peloton_helper
        )

    def setup(self, dynamic_env, instance_number, job_name=None, version=None):
        """
        param dynamic: dict of dynamic environment virable
        param instance_number: number of tasks in the job

        type dynamic: dict
        type instance_number: int

        return: job-id
        """
        if not job_name:
            job_name = self.label + "_" + self.name

        if version:
            self.version = version

        instance_config = {}

        for i in range(instance_number):
            dynamic_env["MESOS_HOSTNAME"] = "-".join(
                [self.label, self.name, str(i), str(uuid.uuid4())]
            )
            instance_config.update(
                {
                    i: create_mesos_task_config(
                        self.config, self.name, dynamic_env, version
                    )
                }
            )

        resp = self.peloton_helper.create_job(
            label=self.label,
            name=job_name,
            default_task_config=instance_config[0],
            instance_config=instance_config,
            num_instance=instance_number,
        )
        self.job_id = resp.jobId.value
        print_okblue("Waiting for job %s setup..." % job_name)
        if not self.peloton_helper.monitering(
            self.job_id, RUNNING_TARGET_STATUS
        ):
            raise ModuleLaunchFailedException("%s can not launch" % self.name)
        return self.job_id


class Cassandra(Module):
    def __init__(self, label_name, config, peloton_helper):
        """
        type param label_name: str
        type config: dict
        type peloton_helper: PelotonClientHelper
        """
        super(Cassandra, self).__init__(
            "cassandra", label_name, config, peloton_helper
        )

    def get_host_port(self):
        """
        rtype: host, port: str, str
        """
        return self.get_host_port_for_instance(0, "CQL_PORT")


class Peloton(Module):
    def __init__(self, label_name, config, peloton_helper):
        """
        type param label_name: str
        type config: dict
        type peloton_helper: PelotonClientHelper
        """
        super(Peloton, self).__init__(
            "peloton", label_name, config, peloton_helper
        )

    def setup(
        self,
        dynamic_env,
        instance_number,
        job_name=None,
        version=None,
        image_path=None,
    ):
        """
        Overrides setup() from base-class to create hostmgr in a phased manner.
        """
        if "hostmgr" not in job_name:
            return super(Peloton, self).setup(
                dynamic_env,
                instance_number,
                job_name=job_name,
                version=version,
                image_path=image_path,
            )

        # create a single instance of hostmgr to avoid running DB migrations
        # concurrently.
        super(Peloton, self).setup(
            dynamic_env,
            1,
            job_name=job_name,
            version=version,
            image_path=image_path,
        )
        # Wait a little so that DB migration can complete.
        # TODO(amitbose) Find a better way to wait
        time.sleep(30)
        jobInfo = self.peloton_helper.get_job(self.job_id).jobInfo
        runtime = jobInfo.runtime
        config = jobInfo.config

        # update the job to change the instances
        config.instanceCount = instance_number
        cl = peloton.ChangeLog(version=runtime.configurationVersion)
        config.changeLog.MergeFrom(cl)
        self.peloton_helper.update_stateless_job(self.job_id, config)

        print_okblue("Waiting for job %s update..." % job_name)
        if not self.peloton_helper.monitering(
            self.job_id, RUNNING_TARGET_STATUS
        ):
            raise ModuleLaunchFailedException(
                "%s can not launch: update failed" % self.name
            )
        return self.job_id
