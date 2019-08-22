#!/usr/bin/env python
from __future__ import absolute_import

from abc import ABCMeta, abstractmethod
import datetime
import grpc
import time
from deepdiff import DeepDiff

from aurora_bridge_client import Client as aurora_bridge_client

from peloton_client.client import PelotonClient
from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.v0.job import job_pb2 as job
from peloton_client.pbgen.peloton.api.v0.respool import respool_pb2 as respool
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.peloton.api.v1alpha import (
    peloton_pb2 as v1alpha_peloton,
)
from peloton_client.pbgen.peloton.api.v1alpha.pod import pod_pb2 as pod
from peloton_client.pbgen.peloton.api.v1alpha.pod.apachemesos import apachemesos_pb2 as apachemesos
from peloton_client.pbgen.peloton.api.v1alpha.job.stateless import (
    stateless_pb2 as stateless,
)
from peloton_client.pbgen.peloton.api.v1alpha.job.stateless.svc import (
    stateless_svc_pb2 as stateless_svc,
)


from m3.client import M3
from m3.emitter import DirectEmitter

default_timeout = 60

RESPOOL_PATH = "DefaultResPool"

INVALID_ENTITY_VERSION_ERR_MESSAGE = "unexpected entity version"


class ResPoolNotFoundException(Exception):
    pass


def create_stateless_job_spec(
    name, labels, instance_count, default_config, respool_id
):
    job_spec = stateless.JobSpec(
        name=name,
        labels=labels,
        owning_team="compute",
        description="test job",
        instance_count=instance_count,
        default_spec=default_config,
        respool_id=v1alpha_peloton.ResourcePoolID(value=respool_id),
        sla=stateless.SlaSpec(priority=1, preemptible=False),
    )
    return job_spec


class Job(object):
    __metaclass__ = ABCMeta

    def __init__(self, client, respool_id):
        self.client = client
        self.respool_id = respool_id
        self.job_id = None

    @abstractmethod
    def create_pod_config(self, sleep_time, dynamic_factor, host_limit_1):
        """
        Create the pod configuration with the sleep time in
        the command and dynamic factor is the echo output.
        """
        pass

    @abstractmethod
    def get_job_info(self):
        """
        Get the job configuration and status.
        """
        pass

    @abstractmethod
    def stop_job(self):
        """
        Kill all tasks of the job.
        """
        pass

    @abstractmethod
    def is_job_killed(self):
        """
        Returns true if all tasks in the job have been killed.
        """
        pass

    @abstractmethod
    def update_job(
        self,
        instance_inc,
        batch_size,
        use_instance_config,
        sleep_time,
        host_limit_1,
    ):
        """
        Update the job. The instance_inc will be added to the instance
        number, batch size is the size to be passed to the update spec,
        per instance configuration is used is use_instance_config is set
        to True and sleep_time is the sleep to be put in the command.
        """
        pass

    @abstractmethod
    def create_job(
        self,
        instance_num,
        use_instance_config,
        sleep_time,
        host_limit_1,
    ):
        """
        Create the job and store the job-id. Number of instances in the
        job are passed via instance_num, per instance configuration is
        used is use_instance_config is set to True and sleep_time is
        the sleep to be put in the command.
        """
        pass

    @abstractmethod
    def get_task_stats(self, job_info):
        """
        Get task state to task count from the job info.
        """
        pass

    @abstractmethod
    def get_labels(self, job_info):
        """
        Get the job labels from the job info.
        """
        pass

    @abstractmethod
    def is_workflow_done(self, job_info):
        """
        Returns true if the workflow running for the job is completed.
        """
        pass

    @abstractmethod
    def get_start_time(self, job_info, create_time):
        """
        Get start time of the job / workflow.
        """
        pass

    @abstractmethod
    def get_completion_time(self, job_info, create_time):
        """
        Get completion time of the job / workflow.
        """
        pass


class BatchJob(Job):
    def create_pod_config(self, sleep_time, dynamic_factor, host_limit_1=False):
        return task.TaskConfig(
            resource=task.ResourceConfig(
                cpuLimit=0.1, memLimitMb=32, diskLimitMb=32
            ),
            command=mesos.CommandInfo(
                shell=True,
                value="echo %s && sleep %s"
                % (str(dynamic_factor), str(sleep_time)),
            ),
        )

    def get_job_info(self):
        request = job.GetRequest(id=peloton.JobID(value=self.job_id))
        resp = self.client.job_svc.Get(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=default_timeout,
        )
        return resp.jobInfo

    def stop_job(self):
        request = task.StopRequest(jobId=peloton.JobID(value=self.job_id))
        self.client.task_svc.Stop(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=default_timeout,
        )

    def is_job_killed(self):
        job_info = self.get_job_info()
        if job_info.runtime.state == job.KILLED:
            return True
        return False

    def update_job(
        self,
        instance_inc,
        batch_size,
        use_instance_config,
        sleep_time,
        host_limit_1=False,
    ):
        job_info = self.get_job_info()
        job_config = job_info.config
        if use_instance_config:
            instance_config = {}
            for i in range(0, instance_inc):
                count = job_config.instanceCount + i
                instance_config[count] = self.create_pod_config(
                    sleep_time, "instance %s" % i
                )
            job_config.instanceConfig.MergeFrom(instance_config)
        job_config.instanceCount = job_config.instanceCount + instance_inc
        request = job.UpdateRequest(
            id=peloton.JobID(value=self.job_id), config=job_config
        )
        self.client.job_svc.Update(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=default_timeout,
        )

    def create_job(
        self,
        instance_num,
        use_instance_config,
        sleep_time,
        host_limit_1=False,
    ):
        default_config = self.create_pod_config(sleep_time, "static")
        instance_config = {}
        if use_instance_config:
            for i in range(0, instance_num):
                instance_config[i] = self.create_pod_config(
                    sleep_time, "instance %s" % i
                )

        request = job.CreateRequest(
            config=job.JobConfig(
                name="instance %s && sleep %s && instance config %s"
                % (instance_num, sleep_time, use_instance_config),
                labels=[
                    peloton.Label(key="task_num", value=str(instance_num)),
                    peloton.Label(key="sleep_time", value=str(sleep_time)),
                    peloton.Label(
                        key="use_instance_config",
                        value=str(use_instance_config),
                    ),
                ],
                owningTeam="compute",
                description="test job",
                instanceCount=instance_num,
                defaultConfig=default_config,
                instanceConfig=instance_config,
                # sla is required by resmgr
                sla=job.SlaConfig(priority=1, preemptible=False),
                respoolID=peloton.ResourcePoolID(value=self.respool_id),
            )
        )

        resp = self.client.job_svc.Create(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=default_timeout,
        )
        self.job_id = resp.jobId.value
        return resp.jobId.value

    def get_task_stats(self, job_info):
        return job_info.runtime.taskStats

    def get_labels(self, job_info):
        return job_info.config.labels

    def is_workflow_done(self, job_info):
        if job_info.runtime.state == job_info.runtime.goalState:
            return True
        return False

    def get_start_time(self, job_info, create_time):
        job_runtime = job_info.runtime
        create_time = datetime.datetime.strptime(
            job_runtime.creationTime[:25], "%Y-%m-%dT%H:%M:%S.%f"
        )

        if job_runtime.startTime:
            start_time = datetime.datetime.strptime(
                job_runtime.startTime[:25], "%Y-%m-%dT%H:%M:%S.%f"
            )
        else:
            start_time = create_time
        return (start_time - create_time).total_seconds()

    def get_completion_time(self, job_info, create_time):
        job_runtime = job_info.runtime
        create_time = datetime.datetime.strptime(
            job_runtime.creationTime[:25], "%Y-%m-%dT%H:%M:%S.%f"
        )

        if job_runtime.completionTime:
            complete_time = datetime.datetime.strptime(
                job_runtime.completionTime[:25], "%Y-%m-%dT%H:%M:%S.%f"
            )
        else:
            complete_time = create_time

        return (complete_time - create_time).total_seconds()


class StatelessJob(Job):
    def create_pod_config(self, sleep_time, dynamic_factor, host_limit_1=False):
        container_spec = pod.ContainerSpec(
            resource=pod.ResourceSpec(
                cpu_limit=0.1,
                mem_limit_mb=32,
                disk_limit_mb=32,
            ),
            entrypoint=pod.CommandSpec(
                value="echo %s && sleep %s"
                % (str(dynamic_factor), str(sleep_time)),
            ),
        )

        instance_label = v1alpha_peloton.Label(
            key="peloton/instance", value="instance-label"
        )
        host_limit_1_constraint = None
        if host_limit_1:
            host_limit_1_constraint = pod.Constraint(
                type=1,  # Label constraint
                label_constraint=pod.LabelConstraint(
                    kind=1,  # Label
                    condition=2,  # Equal
                    requirement=0,
                    label=instance_label,
                ),
            )

        containers = [container_spec]

        mesos_pod_spec = apachemesos.PodSpec(
            shell=True,
        )

        return pod.PodSpec(containers=containers,
                           labels=[instance_label],
                           constraint=host_limit_1_constraint,
                           mesos_spec=mesos_pod_spec)

    def get_job_info(self):
        request = stateless_svc.GetJobRequest(
            job_id=v1alpha_peloton.JobID(value=self.job_id)
        )
        resp = self.client.stateless_svc.GetJob(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=default_timeout,
        )
        return resp.job_info

    def stop_job(self):
        while True:
            # first get the entity version
            job_info = self.get_job_info()
            version = job_info.status.version.value

            request = stateless_svc.StopJobRequest(
                job_id=v1alpha_peloton.JobID(value=self.job_id),
                version=v1alpha_peloton.EntityVersion(value=version),
            )
            try:
                self.client.stateless_svc.StopJob(
                    request,
                    metadata=self.client.jobmgr_metadata,
                    timeout=default_timeout,
                )
            except grpc.RpcError as e:
                # if entity version is incorrect, just retry
                if (
                    e.code() == grpc.StatusCode.ABORTED
                    and INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
                ):
                    continue
                raise
            break

    def is_job_killed(self):
        job_info = self.get_job_info()
        if job_info.status.state == stateless.JOB_STATE_KILLED:
            return True
        return False

    def update_job(
        self,
        instance_inc,
        batch_size,
        use_instance_config,
        sleep_time,
        host_limit_1=False,
    ):
        default_config = self.create_pod_config(
            sleep_time, "static", host_limit_1=host_limit_1)
        job_spec = create_stateless_job_spec(
            "instance %s && sleep %s" % (instance_inc, sleep_time),
            [
                v1alpha_peloton.Label(key="task_num", value=str(instance_inc)),
                v1alpha_peloton.Label(key="sleep_time", value=str(sleep_time)),
            ],
            instance_inc,
            default_config,
            self.respool_id,
        )
        update_spec = stateless.UpdateSpec(batch_size=batch_size)

        while True:
            # first get the entity version
            job_info = self.get_job_info()
            version = job_info.status.version.value
            job_spec.instance_count = (
                job_info.spec.instance_count + instance_inc
            )

            request = stateless_svc.ReplaceJobRequest(
                job_id=v1alpha_peloton.JobID(value=self.job_id),
                version=v1alpha_peloton.EntityVersion(value=version),
                spec=job_spec,
                update_spec=update_spec,
            )
            try:
                resp = self.client.stateless_svc.ReplaceJob(
                    request,
                    metadata=self.client.jobmgr_metadata,
                    timeout=default_timeout,
                )
            except grpc.RpcError as e:
                # if entity version is incorrect, just retry
                if (
                    e.code() == grpc.StatusCode.ABORTED
                    and INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
                ):
                    continue
                raise
            break
        return resp

    def create_job(
        self,
        instance_num,
        use_instance_config,
        sleep_time,
        host_limit_1=False,
    ):
        default_config = self.create_pod_config(
            sleep_time,
            "static",
            host_limit_1=host_limit_1,
        )
        job_spec = create_stateless_job_spec(
            "instance %s && sleep %s" % (instance_num, sleep_time),
            [
                v1alpha_peloton.Label(key="task_num", value=str(instance_num)),
                v1alpha_peloton.Label(key="sleep_time", value=str(sleep_time)),
            ],
            instance_num,
            default_config,
            self.respool_id,
        )
        request = stateless_svc.CreateJobRequest(spec=job_spec)

        resp = self.client.stateless_svc.CreateJob(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=default_timeout,
        )

        self.job_id = resp.job_id.value
        return resp.job_id.value

    def get_task_stats(self, job_info):
        return job_info.status.pod_stats

    def get_labels(self, job_info):
        return job_info.spec.labels

    def is_workflow_done(self, job_info):
        if (
            job_info.status.workflow_status.state
            == stateless.WORKFLOW_STATE_SUCCEEDED
        ):
            return True
        return False

    def get_start_time(self, job_info, create_time):
        # not support for stateless
        return 0

    def get_completion_time(self, job_info, create_time):
        # TODO use creation time and update time from workflow status
        complete_time = datetime.datetime.now()
        # create_time = datetime.datetime.strptime(
        #    workflow_status.creation_time[:25],
        #    '%Y-%m-%dT%H:%M:%S.%f')

        # TODO use completion time instead of update time
        # if workflow_status.update_time:
        #    complete_time = datetime.datetime.strptime(
        #        workflow_status.update_time[:25],
        #        '%Y-%m-%dT%H:%M:%S.%f')
        # else:
        #    complete_time = create_time
        return (complete_time - create_time).total_seconds()


class PerformanceTestClient(object):
    def __init__(self, zk_server, agent_num, version):
        self.zk_server = zk_server
        self.agent_num = agent_num
        self.version = version

        self.m3_client = M3(
            application_identifier="vcluster-monitor",
            emitter=DirectEmitter(),
            environment="production",
            default_tags={
                "peloton_version": self.version,
                "agent_num": str(self.agent_num),
            },
        )

        self.peloton_client = PelotonClient(
            name="peloton-client", zk_servers=zk_server
        )

        self.aurora_bridge_client = aurora_bridge_client(zk_server)

        self.respool_id = self.ensure_respool()

    def get_batch_job(self):
        return BatchJob(self.peloton_client, self.respool_id)

    def get_stateless_job(self):
        return StatelessJob(self.peloton_client, self.respool_id)

    def ensure_respool(self):
        # lookup respool
        request = respool.LookupRequest(
            path=respool.ResourcePoolPath(value="/" + RESPOOL_PATH)
        )
        resp = self.peloton_client.respool_svc.LookupResourcePoolID(
            request,
            metadata=self.peloton_client.resmgr_metadata,
            timeout=default_timeout,
        )

        respool_id = resp.id.value

        if not respool_id:
            raise ResPoolNotFoundException

        return respool_id

    def get_job_info(self, job):
        return job.get_job_info()

    def stop_job(
        self, job, wait_for_kill=False, sleep_time_in_seconds=10, retries=60
    ):
        job.stop_job()
        if wait_for_kill is False:
            return

        # wait for job to be killed before moving forward
        for i in range(retries):
            if job.is_job_killed():
                return
            # sleep and check again
            time.sleep(sleep_time_in_seconds)
        # could not kill, raise exception
        raise Exception("could not kill job")

    def update_job(
        self,
        job,
        instance_inc,
        batch_size,
        use_instance_config,
        sleep_time,
        host_limit_1=False,
    ):
        return job.update_job(
            instance_inc,
            batch_size,
            use_instance_config,
            sleep_time,
            host_limit_1,
        )

    def create_job(self, job, instance_num, use_instance_config, sleep_time, host_limit_1=False):
        return job.create_job(instance_num, use_instance_config, sleep_time, host_limit_1)

    def monitoring_job(self, job, stable_timeout=600):
        """
        monitoring will stop if the job status is not changed in stable_timeout
        or the job status meets the target_status. monitoring returns a bool
        value whether the job completed and meet the target status, and the
        time (seconds) to complete the job.
        rtype: bool, int, int
        """

        if not job:
            return False, 0, 0

        if not job.job_id:
            return False, 0, 0

        data = []
        tags = {}
        stable_timestamp = datetime.datetime.now()
        create_time = datetime.datetime.now()
        while datetime.datetime.now() - stable_timestamp < datetime.timedelta(
            seconds=stable_timeout
        ):
            job_info = job.get_job_info()
            task_stats = job.get_task_stats(job_info)
            data.append(task_stats)

            # Prepare M3 tags and push data to M3
            labels = job.get_labels(job_info)
            for label in labels:
                tags.update({label.key: label.value})

            for state_name, task_num in task_stats.iteritems():
                tags.update({"task_state": state_name})
                self.m3_client.count(
                    key="total_tasks_by_state", n=task_num, tags=tags
                )

            if job.is_workflow_done(job_info):
                break

            if len(data) < 2 or DeepDiff(data[-1], data[-2]):
                # new record is different from previous
                stable_timestamp = datetime.datetime.now()
            time.sleep(10)

        if job.is_workflow_done(job_info) is False:
            return False, 0, 0

        completion_du = job.get_completion_time(job_info, create_time)
        start_du = job.get_start_time(job_info, create_time)
        self.m3_client.timing("start_duration", start_du * 1000, tags)
        self.m3_client.timing("complete_duration", completion_du * 1000, tags)

        return True, start_du, completion_du
