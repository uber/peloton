import logging
import time

from client import Client
from pool import Pool
from task import Task
from google.protobuf import json_format
from peloton_client.pbgen.peloton.api import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.job import job_pb2 as job
from peloton_client.pbgen.peloton.api.task import task_pb2 as task
from peloton_client.pbgen.peloton.api.respool import respool_pb2 as respool
from util import load_test_config


log = logging.getLogger(__name__)


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
    """
    Job represents a peloton job
    """
    def __init__(self, job_file='test_job.yaml',
                 client=None,
                 config=None,
                 pool=None,
                 job_config=None):

        self.config = config or IntegrationTestConfig()
        self.client = client or Client()
        self.pool = pool or Pool(self.config)
        self.job_id = None
        if job_config is None:
            job_config_dump = load_test_config(job_file)
            job_config = job.JobConfig()
            json_format.ParseDict(job_config_dump, job_config)
        self.job_config = job_config

    def create_per_task_configs(self):
        """
        creates task config for each task for the job
        """
        for i in xrange(self.job_config.instanceCount):
            self.job_config.instanceConfig[i].CopyFrom(
                self.job_config.defaultConfig)

    def create(self):
        """
        creates a job based on the config
        :return: the job ID
        """
        respool_id = self.pool.ensure_exists()

        self.job_config.respoolID.value = respool_id
        request = job.CreateRequest(
            config=self.job_config,
        )
        resp = self.client.job_svc.Create(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField('error'), resp
        assert resp.jobId.value
        self.job_id = resp.jobId.value
        log.info('created job %s', self.job_id)

    def update(self, new_job_file):
        """
        updates a job
        :param new_job_file: The job config file used for updating
        """
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

    def start(self, ranges=None):
        """
        Starts a job or certain tasks based on the ranges
        :param ranges: the instance ranges to start
        :return: task start response from the API
        """
        request = task.StartRequest(
            jobId=peloton.JobID(value=self.job_id),
            ranges=ranges,
        )
        response = self.client.task_svc.Start(
                request,
                metadata=self.client.jobmgr_metadata,
                timeout=self.config.rpc_timeout_sec,
            )
        log.info('starting tasks in job {0} with ranges {1}'
                 .format(self.job_id, ranges))
        return response

    def stop(self, ranges=None):
        """
        Stops a job or certain tasks based on the ranges
        :param ranges: the instance ranges to stop
        :return: task stop response from the API
        """
        request = task.StopRequest(
            jobId=peloton.JobID(value=self.job_id),
            ranges=ranges,
        )
        response = self.client.task_svc.Stop(
                request,
                metadata=self.client.jobmgr_metadata,
                timeout=self.config.rpc_timeout_sec,
            )
        log.info('stopping tasks in job {0} with ranges {1}'
                 .format(self.job_id, ranges))
        return response

    def get_info(self):
        """
        :return: The job info
        """
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
        """
        :return: The job runtime from the job info
        """
        return self.get_info().runtime

    def get_config(self):
        """
        :return: The job config from the job info
        """
        return self.get_info().config

    def get_task(self, instance_id):
        """
        :param instance_id: The instance id of the task
        :return: The Task of the job based on the instance id
        """
        return Task(self, instance_id)

    def get_tasks(self):
        """
        :return: All the tasks of the job
        """
        config = self.get_config()
        return {iid: Task(self, iid) for iid in xrange(config.instanceCount)}

    def get_task_info(self, instance_id):
        """
        :param instance_id: The instance id of the task
        :return: The task info for the instance id
        """
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
        """
        :param instance_id: The instance id of the task
        :return: Returns all active and completed tasks of the given instance
        """
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
        """
        :param instance_id: The instance id of the task
        :param task_id: The mesos task id of the task
        :return: The BrowseSandboxResponse
        """
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

    def list_tasks(self):
        """
        :return: The map of instance ID to task info for all matching tasks
        """
        request = task.ListRequest(
            jobId=peloton.JobID(value=self.job_id),
        )
        resp = self.client.task_svc.List(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField('notFound')
        return resp.result

    def wait_for_terminated(self):
        """
        Waits for the job to be terminated
        """
        state = ''
        attempts = 0
        log.info('%s waiting for terminal state', self.job_id)
        terminated = False
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
                    log.info('%s transitioned to state %s', self.job_id,
                             new_state)
                state = new_state
                if state in ['SUCCEEDED', 'FAILED', 'KILLED']:
                    terminated = True
                    break
                log.debug(format_stats(runtime.taskStats))
            except Exception as e:
                log.warn(e)
            finally:
                time.sleep(self.config.sleep_time_sec)
                attempts += 1
        if terminated:
            log.info('%s job terminated', self.job_id)
            assert True

        if attempts == self.config.max_retry_attempts:
            log.info('% max attempts reached to wait for goal state',
                     self.job_id)
            log.info('current_state:%s', state)
            assert False

    def wait_for_state(self, goal_state='SUCCEEDED', failed_state='FAILED'):
        """
        Waits for the job to reach a particular state
        :param goal_state: The state to reach
        :param failed_state: The failed state of the job
        """
        state = ''
        attempts = 0
        start = time.time()
        log.info('%s waiting for state %s', self.job_id, goal_state)
        state_transition_failure = False
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
                    log.info('%s transitioned to state %s', self.job_id,
                             new_state)
                state = new_state
                if state == goal_state:
                    break
                log.debug(format_stats(runtime.taskStats))
                # If we assert here, we will log the exception,
                # and continue with the finally block. Set a flag
                # here to indicate failure and then break the loop
                # in the finally block
                if state == failed_state:
                    state_transition_failure = True
            except Exception as e:
                log.warn(e)
            finally:
                if state_transition_failure:
                    break
                time.sleep(self.config.sleep_time_sec)
                attempts += 1

        if state_transition_failure:
            log.info('goal_state:%s current_state:%s attempts: %s',
                     goal_state, state, str(attempts))
            assert False

        if attempts == self.config.max_retry_attempts:
            log.info('%s max attempts reached to wait for goal state',
                     self.job_id)
            log.info('goal_state:%s current_state:%s', goal_state, state)
            assert False

        end = time.time()
        elapsed = end - start
        log.info('%s state transition took %s seconds', self.job_id, elapsed)
        assert state == goal_state

    def wait_for_condition(self, condition):
        """
        Waits for a particular condition to be met with the job
        :param condition: The condition to meet
        """
        attempts = 0
        start = time.time()
        log.info('%s waiting for condition %s', self.job_id,
                 condition.__name__)
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
        log.info('%s waited on condition %s for %s seconds',
                 self.job_id, condition.__name__, elapsed)
        assert result

    def update_instance_count(self, count):
        """
        Updates the instance count of a job
        :param count: The new count
        """
        self.job_config.instanceCount = count
        self.job_config.sla.maximumRunningInstances = count


def kill_jobs(jobs):
    """
    Kills all the jobs
    :param jobs: The jobs to kill
    """
    for j in jobs:
        j.stop()

    for j in jobs:
        j.wait_for_terminated()


def format_stats(stats):
    return ' '.join((
        '%s: %s' % (name.lower(), stats[name])
        for name in job.JobState.keys()
    ))
