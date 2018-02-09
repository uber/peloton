#!/usr/bin/env python
from __future__ import absolute_import

import datetime
import time
from deepdiff import DeepDiff

from peloton_client.client import PelotonClient
from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from peloton_client.pbgen.peloton.api import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.job import job_pb2 as job
from peloton_client.pbgen.peloton.api.respool import respool_pb2 as respool
from peloton_client.pbgen.peloton.api.task import task_pb2 as task


default_timeout = 60

RESPOOL_PATH = 'DefaultResPool'


def create_pool_config(cpu, memory, disk):
    return respool.ResourcePoolConfig(
                name=RESPOOL_PATH,
                resources=[
                    respool.ResourceConfig(
                        kind='cpu',
                        reservation=cpu,
                        limit=cpu,
                        share=1,
                    ),
                    respool.ResourceConfig(
                        kind='memory',
                        reservation=memory,
                        limit=memory,
                        share=1,
                    ),
                    respool.ResourceConfig(
                        kind='disk',
                        reservation=disk,
                        limit=disk,
                        share=1,
                    ),
                ],
                parent=peloton.ResourcePoolID(value='root')
            )


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
    def __init__(self, zk_server):
        self.zk_server = zk_server
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

        # create pool if not exist
        if resp.id.value is None or resp.id.value == u'':
            respool_config = create_pool_config(4, 12, 12)
            request = respool.CreateRequest(
                config=respool_config,
            )
            resp = self.client.respool_svc.CreateResourcePool(
                request,
                metadata=self.client.resmgr_metadata,
                timeout=default_timeout,
            )
            respool_id = resp.result.value
        else:
            respool_id = resp.id.value

        assert respool_id
        return respool_id

    def get_job_status(self, job_id):
        request = job.GetRequest(
            id=peloton.JobID(value=job_id),
        )
        try:
            resp = self.client.job_svc.Get(
                request,
                metadata=self.client.jobmgr_metadata,
                timeout=default_timeout,
            )
            return resp.jobInfo.runtime
        except Exception:
            raise

    def run_benchmark(self,
                      instance_num,
                      sleep_time,
                      use_instance_config=False):
        default_config = create_task_config(sleep_time, 'static')
        instance_config = {}
        if use_instance_config:
            for i in range(0, instance_num):
                instance_config[i] = create_task_config(sleep_time,
                                                        'instance %s' % i)

        request = job.CreateRequest(
            config=job.JobConfig(
                name='test job',
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

        try:
            resp = self.client.job_svc.Create(
                request,
                metadata=self.client.jobmgr_metadata,
                timeout=default_timeout,
            )
            print resp
        except Exception:
            raise

        target_status = {
            'SUCCEEDED': instance_num
        }
        return self.monitering(resp.jobId.value, target_status)

    def monitering(self, job_id, target_status, stable_timeout=360):
        """
        monitering will stop if the job status is not changed in stable_timeout
        or the job status meets the target_status. monitering returns a bool
        value whether the job completedd and meet the target status, and the
        time (seconds) to complete the job.

        rtype: bool, int, int

        """
        if not job_id:
            return
        data = []

        def check_finish(task_stats):
            for k, v in target_status.iteritems():
                if task_stats.get(k, 0) < v:
                    return False
            return True

        stable_timestamp = datetime.datetime.now()
        while datetime.datetime.now() - stable_timestamp < datetime.timedelta(
                seconds=stable_timeout):
            job_runtime = self.get_job_status(job_id)
            task_stats = dict(job_runtime.taskStats)
            data.append(task_stats)
            if check_finish(task_stats):
                break
            if len(data) < 2 or DeepDiff(data[-1], data[-2]):
                # new record is different from previous
                stable_timestamp = datetime.datetime.now()
            time.sleep(5)

        if not check_finish(task_stats):
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

        start_duration = (start_time - create_time).total_seconds()
        complete_duration = (complete_time - create_time).total_seconds()

        return True, start_duration, complete_duration
