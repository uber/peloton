import logging
import time

from client import Client
from task import Task
from google.protobuf import json_format
from peloton_client.pbgen.peloton.api import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.job import job_pb2 as job
from peloton_client.pbgen.peloton.api.task import task_pb2 as task
from peloton_client.pbgen.peloton.api.respool import respool_pb2 as respool
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
    def __init__(self, job_file='test_job.yaml',
                 client=None,
                 config=None,
                 job_config=None):

        self.config = config or IntegrationTestConfig()
        self.client = client or Client()
        self.job_id = None
        if job_config is None:
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
        self.job_id = resp.jobId.value
        log.info('created job %s', self.job_id)

    def update(self, new_job_file):
        job_config_dump = load_test_config(new_job_file)
        new_job_config = job.JobConfig()
        json_format.ParseDict(job_config_dump, new_job_config)

        request = job.UpdateRequest(
            id=peloton.JobID(value=self.job_id),
            config=new_job_config,
        )
        resp = self.client.job_svc.Update(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField('error')

        # update the config
        self.job_config = new_job_config
        log.info('updated job %s', self.job_id)

    def start(self):
        request = task.StartRequest(
            jobId=peloton.JobID(value=self.job_id),
        )
        self.client.task_svc.Start(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        log.info('starting all tasks in job %s', self.job_id)

    def stop(self):
        request = task.StopRequest(
            jobId=peloton.JobID(value=self.job_id),
        )
        self.client.task_svc.Stop(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        log.info('stopping all tasks in job %s', self.job_id)

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

    def get_task_runs(self, instance_id):
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
        return resp.results

    def browse_task_sandbox(self, instance_id, task_id):
        request = task.BrowseSandboxRequest(
            jobId=peloton.JobID(value=self.job_id),
            instanceId=instance_id,
            taskId=task_id
        )
        resp = self.client.task_svc.BrowseSandbox(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField('error')
        return resp

    def wait_for_state(self, goal_state='SUCCEEDED', failed_state='FAILED'):
        state = ''
        attempts = 0
        start = time.time()
        log.info('waiting for state %s', goal_state)
        while attempts < self.config.max_retry_attempts:
            try:
                request = job.GetRequest(
                    id=peloton.JobID(value=self.job_id),
                )
                resp = self.client.job_svc.Get(
                    request,
                    metadata=self.client.jobmgr_metadata,
                    timeout=self.config.rpc_timeout_sec,
                )
                runtime = resp.jobInfo.runtime
                new_state = job.JobState.Name(runtime.state)
                if state != new_state:
                    log.info('transitioned to state %s', new_state)
                state = new_state
                if state == goal_state:
                    break
                log.debug(format_stats(runtime.taskStats))
                assert state != failed_state
            except Exception as e:
                log.warn(e)
            finally:
                time.sleep(self.config.sleep_time_sec)
                attempts += 1

        if attempts == self.config.max_retry_attempts:
            log.info('max attempts reached to wait for goal state')
            log.info('goal_state:%s current_state:%s', goal_state, state)
            assert False

        end = time.time()
        elapsed = end - start
        log.info('state transition took %s seconds', elapsed)
        assert state == goal_state

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
            log.info('condition: %s', condition.__name__)
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
