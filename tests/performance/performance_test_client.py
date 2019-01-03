#!/usr/bin/env python
from __future__ import absolute_import

import datetime
import time
from deepdiff import DeepDiff

from peloton_client.client import PelotonClient
from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.v0.job import job_pb2 as job
from peloton_client.pbgen.peloton.api.v0.respool import respool_pb2 as respool
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task

from m3.client import M3
from m3.emitter import DirectEmitter

default_timeout = 60

RESPOOL_PATH = 'DefaultResPool'


class ResPoolNotFoundException(Exception):
    pass


def create_task_config(sleep_time, dynamic_factor):
    return task.TaskConfig(
        resource=task.ResourceConfig(
            cpuLimit=0.1,
            memLimitMb=32,
            diskLimitMb=32,
        ),
        command=mesos.CommandInfo(
            shell=True,
            value="echo %s && sleep %s" % (
                str(dynamic_factor), str(sleep_time)),
        ),
    )


class PerformanceTestClient(object):
    def __init__(self, zk_server, agent_num, version):
        self.zk_server = zk_server
        self.agent_num = agent_num
        self.version = version

        self.m3_client = M3(
            application_identifier='vcluster-monitor',
            emitter=DirectEmitter(),
            environment='production',
            default_tags={
                'peloton_version': self.version,
                'agent_num': str(self.agent_num),
            }
        )

        self.client = PelotonClient(
            name='peloton-client',
            zk_servers=zk_server,
        )
        self.respool_id = self.ensure_respool()

    def ensure_respool(self):
        # lookup respool
        request = respool.LookupRequest(
            path=respool.ResourcePoolPath(value='/' + RESPOOL_PATH),
        )
        resp = self.client.respool_svc.LookupResourcePoolID(
            request,
            metadata=self.client.resmgr_metadata,
            timeout=default_timeout,
        )

        respool_id = resp.id.value

        if not respool_id:
            raise ResPoolNotFoundException

        return respool_id

    def get_job_info(self, job_id):
        request = job.GetRequest(
            id=peloton.JobID(value=job_id),
        )
        resp = self.client.job_svc.Get(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=default_timeout,
        )
        return resp.jobInfo

    def stop_job(self, job_id):
        request = task.StopRequest(
            jobId=peloton.JobID(value=job_id),
        )
        self.client.task_svc.Stop(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=default_timeout,
        )

    def update_job(self, job_id, instance_inc, use_instance_config,
                   sleep_time):
        job_info = self.get_job_info(job_id)
        job_config = job_info.config
        if use_instance_config:
            instance_config = {}
            for i in range(0, instance_inc):
                count = job_config.instanceCount + i
                instance_config[count] = create_task_config(sleep_time,
                                                            'instance %s' % i)
            job_config.instanceConfig.MergeFrom(instance_config)
        job_config.instanceCount = job_config.instanceCount + instance_inc
        request = job.UpdateRequest(
            id=peloton.JobID(value=job_id),
            config=job_config,
        )
        self.client.job_svc.Update(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=default_timeout,
        )

    def create_job(self, instance_num, sleep_time, use_instance_config=False):
        default_config = create_task_config(sleep_time, 'static')
        instance_config = {}
        if use_instance_config:
            for i in range(0, instance_num):
                instance_config[i] = create_task_config(sleep_time,
                                                        'instance %s' % i)

        request = job.CreateRequest(
            config=job.JobConfig(
                name='instance %s && sleep %s && instance config %s' % (
                    instance_num, sleep_time, use_instance_config),
                labels=[
                    peloton.Label(
                        key='task_num',
                        value=str(instance_num),
                    ),
                    peloton.Label(
                        key='sleep_time',
                        value=str(sleep_time),
                    ),
                    peloton.Label(
                        key='use_instance_config',
                        value=str(use_instance_config),
                    ),
                ],
                owningTeam='compute',
                description='test job',
                instanceCount=instance_num,
                defaultConfig=default_config,
                instanceConfig=instance_config,
                # sla is required by resmgr
                sla=job.SlaConfig(
                    priority=1,
                    preemptible=True,
                ),
                respoolID=peloton.ResourcePoolID(value=self.respool_id),
            ),
        )

        resp = self.client.job_svc.Create(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=default_timeout,
        )
        return resp.jobId.value

    def monitoring(self, job_id, stable_timeout=600):
        """
        monitering will stop if the job status is not changed in stable_timeout
        or the job status meets the target_status. monitering returns a bool
        value whether the job completedd and meet the target status, and the
        time (seconds) to complete the job.
        rtype: bool, int, int
        """
        if not job_id:
            return False, 0, 0
        data = []
        tags = {}
        stable_timestamp = datetime.datetime.now()
        while datetime.datetime.now() - stable_timestamp < datetime.timedelta(
                seconds=stable_timeout):
            job_info = self.get_job_info(job_id)
            job_runtime = job_info.runtime
            job_config = job_info.config
            task_stats = dict(job_runtime.taskStats)
            data.append(task_stats)

            # Prepare M3 tags and push data to M3
            for label in job_config.labels:
                tags.update({label.key: label.value})

            for state_name, task_num in task_stats.iteritems():
                tags.update({'task_state': state_name})
                self.m3_client.count(
                    key='total_tasks_by_state',
                    n=task_num,
                    tags=tags
                )

            if job_runtime.state == job_runtime.goalState:
                break

            if len(data) < 2 or DeepDiff(data[-1], data[-2]):
                # new record is different from previous
                stable_timestamp = datetime.datetime.now()
            time.sleep(10)

        if job_runtime.state != job_runtime.goalState:
            return False, 0, 0

        create_time = datetime.datetime.strptime(
            job_runtime.creationTime[:25],
            '%Y-%m-%dT%H:%M:%S.%f')

        if job_runtime.startTime:
            start_time = datetime.datetime.strptime(
                job_runtime.startTime[:25],
                '%Y-%m-%dT%H:%M:%S.%f')
        else:
            start_time = create_time

        if job_runtime.completionTime:
            complete_time = datetime.datetime.strptime(
                job_runtime.completionTime[:25],
                '%Y-%m-%dT%H:%M:%S.%f')
        else:
            complete_time = create_time

        start_du = (start_time - create_time).total_seconds()
        complete_du = (complete_time - create_time).total_seconds()

        self.m3_client.timing(
            'start_duration', start_du * 1000, tags)
        self.m3_client.timing(
            'complete_duration', complete_du * 1000, tags)

        return True, start_du, complete_du
