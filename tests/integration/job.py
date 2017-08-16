import logging
import time

from client import Client
from task import Task
from google.protobuf import json_format
from peloton_client.pbgen.peloton.api import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.job import job_pb2 as job
from peloton_client.pbgen.peloton.api.respool import respool_pb2 as respool
from peloton_client.pbgen.peloton.api.task import task_pb2 as task
from peloton_client.pbgen.peloton.api.upgrade import upgrade_pb2 as upgrade
from peloton_client.pbgen.peloton.api.upgrade.svc import (
    upgrade_svc_pb2 as upgrade_svc)
from util import load_test_config


log = logging.getLogger(__name__)


RESPOOL_ROOT = '/'


class IntegrationTestConfig(object):
    def __init__(self, pool_file='test_respool.yaml', max_retry_attempts=60,
                 sleep_time_sec=1, rpc_timeout_sec=10):
        respool_config_dump = load_test_config(pool_file)
        respool_config = respool.ResourcePoolConfig()
        json_format.ParseDict(respool_config_dump, respool_config)
        self.respool_config = respool_config

        self.max_retry_attempts = max_retry_attempts
        self.sleep_time_sec = sleep_time_sec
        self.rpc_timeout_sec = rpc_timeout_sec


class Job(object):
    def __init__(self, job_file='test_job.yaml', client=None, config=None):

        self.config = config or IntegrationTestConfig()
        self.client = client or Client()
        self.job_id = None

        job_config_dump = load_test_config(job_file)
        job_config = job.JobConfig()
        json_format.ParseDict(job_config_dump, job_config)
        self.job_config = job_config

    def create_per_task_configs(self):
        for i in xrange(self.job_config.instanceCount):
            self.job_config.instanceConfig[i].CopyFrom(
                self.job_config.defaultConfig)

    def create(self):
        respool_id = self.ensure_respool()

        self.job_config.respoolID.value = respool_id
        request = job.CreateRequest(
            config=self.job_config,
        )
        resp = self.client.job_svc.Create(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert resp.jobId.value
        assert not resp.HasField('error')
        self.job_id = resp.jobId.value
        log.info('created job %s', self.job_id)

    def start(self, tasks=None):
        ranges = compose_instance_ranges(tasks)
        request = task.StartRequest(
            jobId=peloton.JobID(value=self.job_id),
            ranges=ranges,
        )
        resp = self.client.task_svc.Start(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField('error')
        if tasks:
            log.info('starting tasks %s in job %s',
                     get_tasks_instance_ids(tasks), self.job_id)
        else:
            log.info('starting all tasks in job %s', self.job_id)

    def stop(self, tasks=None):
        ranges = compose_instance_ranges(tasks)
        request = task.StopRequest(
            jobId=peloton.JobID(value=self.job_id),
            ranges=ranges,
        )
        resp = self.client.task_svc.Stop(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField('error')
        if tasks:
            log.info('stopping tasks %s in job %s',
                     get_tasks_instance_ids(tasks), self.job_id)
        else:
            log.info('stopping all tasks in job %s', self.job_id)

    def restart(self, tasks=None):
        ranges = compose_instance_ranges(tasks)
        request = task.RestartRequest(
            jobId=peloton.JobID(value=self.job_id),
            ranges=ranges,
        )
        resp = self.client.task_svc.Restart(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField('notFound')
        assert not resp.HasField('outOfRange')
        if tasks:
            log.info('restarting tasks %s in job %s',
                     get_tasks_instance_ids(tasks), self.job_id)
        else:
            log.info('restarting all tasks in job %s', self.job_id)

    def get_info(self):
        request = job.GetRequest(
            id=peloton.JobID(value=self.job_id),
        )
        resp = self.client.job_svc.Get(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField('error')
        return resp.jobInfo

    def get_runtime(self):
        return self.get_info().runtime

    def get_config(self):
        return self.get_info().config

    def get_task(self, iid):
        return Task(self, iid)

    def get_tasks(self, one_range=None):
        config = self.get_config()
        return {iid: Task(self, iid) for iid in xrange(config.instanceCount)}

    def get_task_info(self, instance_id):
        request = task.GetRequest(
            jobId=peloton.JobID(value=self.job_id),
            instanceId=instance_id,
        )
        resp = self.client.task_svc.Get(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField('notFound')
        assert not resp.HasField('outOfRange')
        return resp.result

    def update(self, config):
        request = job.UpdateRequest(
            id=peloton.JobID(value=self.job_id),
            config=config,
        )
        resp = self.client.job_svc.Update(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField('error')
        self.job_config = config

    def upgrade(self, config):
        request = upgrade_svc.CreateRequest(
            jobId=peloton.JobID(value=self.job_id),
            jobConfig=config,
            options=upgrade.Options()
        )
        res = self.client.upgrade_svc.Create(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        ).id
        log.info('started upgrade of job %s', self.job_id)
        return res

    def wait_for_state(self, goal_state='SUCCEEDED', failed_state='FAILED',
                       task_count=None):
        def make_state(s, c):
            return {'state': s, 'count': c}

        state = make_state('', 0)

        def check_state():
            runtime = self.get_runtime()
            new_state = make_state(job.JobState.Name(runtime.state),
                                   runtime.taskStats[goal_state])
            log.debug(format_stats(runtime.taskStats))

            if state != new_state:
                log.info('transitioned to state %s(%d)',
                         new_state['state'], new_state['count'])
                state.update(new_state)

            if state == make_state(goal_state, task_count or state['count']):
                return True

            assert state['state'] != failed_state
            return False

        check_state.__name__ = 'state {}({})'.format(
            goal_state, task_count or 'any')
        self.wait_for_condition(check_state)
        assert state['state'] == goal_state

    def wait_for_condition(self, condition):
        attempts = 0
        start = time.time()
        log.info('waiting for condition %s', condition.__name__)
        result = False
        while attempts < self.config.max_retry_attempts:
            try:
                result = condition()
                if result:
                    break
            except Exception as e:
                log.warn(e)

            time.sleep(self.config.sleep_time_sec)
            attempts += 1

        if attempts == self.config.max_retry_attempts:
            log.info('max attempts reached to wait for condition')
            log.info('confition: %s', condition.__name__)
            assert False

        end = time.time()
        elapsed = end - start
        log.info('waited on condition %s for %s seconds',
                 condition.__name__, elapsed)
        assert result

    def ensure_respool(self):
        # lookup respool
        respool_name = self.config.respool_config.name
        request = respool.LookupRequest(
            path=respool.ResourcePoolPath(value=RESPOOL_ROOT + respool_name),
        )
        resp = self.client.respool_svc.LookupResourcePoolID(
            request,
            metadata=self.client.resmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        if resp.id.value is None or resp.id.value == u'':
            request = respool.CreateRequest(
                config=self.config.respool_config,
            )
            resp = self.client.respool_svc.CreateResourcePool(
                request,
                metadata=self.client.resmgr_metadata,
                timeout=self.config.rpc_timeout_sec,
            )
            id = resp.result.value
        else:
            id = resp.id.value

        assert id
        return id


def format_stats(stats):
    return ' '.join((
        '%s: %s' % (name.lower(), stats[name])
        for name in job.JobState.keys()
    ))


def get_tasks_instance_ids(tasks):
    """Extract all unique tasks ids and sort them"""
    if not tasks:
        return None

    # Normalize to sequence
    if isinstance(tasks, dict):
        tasks = tasks.values()
    elif not isinstance(tasks, (list, tuple, set)):
        tasks = (tasks,)

    # Make sure that all objects are of proper type
    assert all(isinstance(t, Task) for t in tasks)
    return sorted(set(t.instance_id for t in tasks))


def create_instance_range(first, last):
    """Create instance range [first, last]"""
    return task.InstanceRange(**{'from': first, 'to': last+1})


def compose_instance_ranges(tasks):
    """Compose continuous instance ranges from a task set"""
    if not tasks:
        return None

    # Create a list of instance ranges
    start, end, ranges = -1, -1, []
    for i, instance_id in enumerate(get_tasks_instance_ids(tasks)):
        if i == 0:
            start, end = instance_id, instance_id
            continue

        if end + 1 != instance_id:
            # Append new range
            ranges.append(create_instance_range(start, end))
            start, end = instance_id, instance_id
        else:
            # Extend the range
            end = instance_id

    # Append the last range
    ranges.append(create_instance_range(start, end))
    return ranges
