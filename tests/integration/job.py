import logging
import time
import grpc

from client import Client
from pool import Pool
from task import Task
from workflow import Workflow
from common import IntegrationTestConfig, wait_for_condition
from util import load_test_config

from google.protobuf import json_format

from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.v0.job import job_pb2 as job
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task


log = logging.getLogger(__name__)


# Options to mutate the job config


def with_labels(labels):
    def apply(job_config):
        for lk, lv in labels.iteritems():
            job_config.defaultConfig.labels.extend(
                [peloton.Label(key=lk, value=lv)]
            )

    return apply


def with_constraint(constraint):
    def apply(job_config):
        job_config.defaultConfig.constraint.CopyFrom(constraint)

    return apply


def with_instance_count(count):
    def apply(job_config):
        job_config.instanceCount = count

    return apply


def with_job_name(name):
    def apply(job_config):
        job_config.name = name

    return apply


class Job(object):
    """
    Job represents a peloton job
    """

    def __init__(
        self,
        job_file="test_job.yaml",
        client=None,
        config=None,
        pool=None,
        job_config=None,
        options=[],
        job_id=None,
    ):

        self.config = config or IntegrationTestConfig()
        self.client = client or Client()
        self.pool = pool or Pool(self.config, self.client)
        self.job_id = job_id
        if job_config is None:
            job_config_dump = load_test_config(job_file)
            job_config = job.JobConfig()
            json_format.ParseDict(job_config_dump, job_config)

        # apply options
        for o in options:
            o(job_config)

        self.job_config = job_config

    def create(self):
        """
        creates a job based on the config
        :return: the job ID
        """
        respool_id = self.pool.ensure_exists()
        self.job_config.respoolID.value = respool_id

        # wait for job manager leader
        self.wait_for_jobmgr_available()
        request = job.CreateRequest(config=self.job_config)
        resp = self.client.job_svc.Create(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField("error"), resp
        assert resp.jobId.value
        self.job_id = resp.jobId.value
        log.info("created job %s", self.job_id)

    def update(self, new_job_file):
        """
        updates a job
        :param new_job_file: The job config file used for updating
        """
        # wait for job manager leader
        self.wait_for_jobmgr_available()
        job_config_dump = load_test_config(new_job_file)
        new_job_config = job.JobConfig()
        json_format.ParseDict(job_config_dump, new_job_config)

        request = job.UpdateRequest(
            id=peloton.JobID(value=self.job_id), config=new_job_config
        )
        resp = self.client.job_svc.Update(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField("error")

        # update the config
        self.job_config = new_job_config
        log.info("updated job %s", self.job_id)

    def start(self, ranges=None):
        """
        Starts a job or certain tasks based on the ranges
        :param ranges: the instance ranges to start
        :return: task start response from the API
        """
        # wait for job manager leader
        self.wait_for_jobmgr_available()
        request = task.StartRequest(
            jobId=peloton.JobID(value=self.job_id), ranges=ranges
        )
        response = self.client.task_svc.Start(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        log.info(
            "starting tasks in job {0} with ranges {1}".format(
                self.job_id, ranges
            )
        )
        return response

    def stop(self, ranges=None):
        """
        Stops a job or certain tasks based on the ranges
        :param ranges: the instance ranges to stop
        :return: task stop response from the API
        """
        # wait for job manager leader
        self.wait_for_jobmgr_available()
        request = task.StopRequest(
            jobId=peloton.JobID(value=self.job_id), ranges=ranges
        )
        response = self.client.task_svc.Stop(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        log.info(
            "stopping tasks in job {0} with ranges {1}".format(
                self.job_id, ranges
            )
        )
        return response

    def delete(self):
        """
        Deletes a job
        :return: delete job response from the API
        """
        # wait for job manager leader
        self.wait_for_jobmgr_available()
        request = job.DeleteRequest(id=peloton.JobID(value=self.job_id))
        response = self.client.job_svc.Delete(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not response.HasField("error")

        log.info("deleting job {0}".format(self.job_id))
        return response

    class WorkflowResp:
        """
        WorkflowResp represents the response from a job level operation
        including update, restart, stop and start.
        """

        def __init__(
            self, workflow_id, resource_version, client=None, config=None
        ):
            self.workflow = Workflow(workflow_id, client=client, config=config)
            self.resource_version = resource_version

    def rolling_start(
        self, ranges=None, resource_version=None, batch_size=None
    ):
        """
        Starts a job or certain tasks in a rolling fashion based on the ranges
        and batch size
        :param ranges: the instance ranges to start
        :param resource_version: the resource_version to use,
            if not set. the API would fetch if from job runtime
        :param batch_size: the batch size of rolling start
       :return: WorkflowResp
       """
        if resource_version is None:
            job_info = self.get_info()
            resource_version = job_info.runtime.configurationVersion

        request = job.StartRequest(
            id=peloton.JobID(value=self.job_id),
            ranges=ranges,
            resourceVersion=resource_version,
            startConfig=job.StartConfig(batchSize=batch_size),
        )
        resp = self.client.job_svc.Start(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        return Job.WorkflowResp(
            resp.updateID.value,
            resp.resourceVersion,
            client=self.client,
            config=self.config,
        )

    def rolling_stop(
        self, ranges=None, resource_version=None, batch_size=None
    ):
        """
        Stops a job or certain tasks in a rolling fashion based on the ranges
        and batch size
        :param ranges: the instance ranges to stop
        :param resource_version: the resource_version to use,
            if not set. the API would fetch if from job runtime
        :param batch_size: the batch size of rolling stop
       :return: WorkflowResp
       """
        if resource_version is None:
            job_info = self.get_info()
            resource_version = job_info.runtime.configurationVersion

        request = job.StopRequest(
            id=peloton.JobID(value=self.job_id),
            ranges=ranges,
            resourceVersion=resource_version,
            stopConfig=job.StopConfig(batchSize=batch_size),
        )
        resp = self.client.job_svc.Stop(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        return Job.WorkflowResp(
            resp.updateID.value,
            resp.resourceVersion,
            client=self.client,
            config=self.config,
        )

    def rolling_restart(
        self, ranges=None, resource_version=None, batch_size=None
    ):
        """
        Restart a job or certain tasks in a rolling fashion based on the ranges
        and batch size
        :param ranges: the instance ranges to stop
        :param resource_version: the resource_version to use,
            if not set. the API would fetch if from job runtime
        :param batch_size: the batch size of rolling stop
       :return: WorkflowResp
       """
        if resource_version is None:
            job_info = self.get_info()
            resource_version = job_info.runtime.configurationVersion

        request = job.RestartRequest(
            id=peloton.JobID(value=self.job_id),
            ranges=ranges,
            resourceVersion=resource_version,
            restartConfig=job.RestartConfig(batchSize=batch_size),
        )
        resp = self.client.job_svc.Restart(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        return Job.WorkflowResp(
            resp.updateID.value,
            resp.resourceVersion,
            client=self.client,
            config=self.config,
        )

    def get_info(self):
        """
        :return: The job info
        """
        request = job.GetRequest(id=peloton.JobID(value=self.job_id))
        resp = self.client.job_svc.Get(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField("error")
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
            jobId=peloton.JobID(value=self.job_id), instanceId=instance_id
        )
        resp = self.client.task_svc.Get(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField("notFound")
        assert not resp.HasField("outOfRange")
        return resp.result

    def get_task_runs(self, instance_id):
        """
        :param instance_id: The instance id of the task
        :return: Returns all active and completed tasks of the given instance
        """
        request = task.GetRequest(
            jobId=peloton.JobID(value=self.job_id), instanceId=instance_id
        )
        resp = self.client.task_svc.Get(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField("notFound")
        assert not resp.HasField("outOfRange")
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
            taskId=task_id,
        )
        resp = self.client.task_svc.BrowseSandbox(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField("error")
        return resp

    def list_tasks(self):
        """
        :return: The map of instance ID to task info for all matching tasks
        """
        request = task.ListRequest(jobId=peloton.JobID(value=self.job_id))
        resp = self.client.task_svc.List(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        assert not resp.HasField("notFound")
        return resp.result

    def get_pod_events(self, instance_id):
        """
        :return: A list of all of the pod events of a given task
        """
        request = task.GetPodEventsRequest(
            jobId=peloton.JobID(value=self.job_id), instanceId=instance_id
        )
        resp = self.client.task_svc.GetPodEvents(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        return resp.result

    def wait_for_all_tasks_running(self):
        attempts = 0
        start = time.time()
        while attempts < self.config.max_retry_attempts:
            try:
                count = 0
                task_infos = self.list_tasks().value
                for instance_id, task_info in task_infos.items():
                    if task_info.runtime.state == task.RUNNING:
                        count += 1

                if count == len(task_infos):
                    log.info("%s job has %s running tasks", self.job_id, count)
                    break
            except Exception as e:
                log.warn(e)

            time.sleep(self.config.sleep_time_sec)
            attempts += 1

        if attempts == self.config.max_retry_attempts:
            log.info("max attempts reached to wait for all tasks running")
            assert False

        end = time.time()
        elapsed = end - start
        log.info(
            "%s job has all running tasks in %s seconds", self.job_id, elapsed
        )

    def wait_for_terminated(self):
        """
        Waits for the job to be terminated
        """
        state = ""
        attempts = 0
        log.info("%s waiting for terminal state", self.job_id)
        terminated = False
        while attempts < self.config.max_retry_attempts:
            try:
                request = job.GetRequest(id=peloton.JobID(value=self.job_id))
                resp = self.client.job_svc.Get(
                    request,
                    metadata=self.client.jobmgr_metadata,
                    timeout=self.config.rpc_timeout_sec,
                )
                runtime = resp.jobInfo.runtime
                new_state = job.JobState.Name(runtime.state)
                if state != new_state:
                    log.info(
                        "%s transitioned to state %s", self.job_id, new_state
                    )
                state = new_state
                if state in ["SUCCEEDED", "FAILED", "KILLED"]:
                    terminated = True
                    break
                log.debug(format_stats(runtime.taskStats))
            except Exception as e:
                log.warn(e)
            finally:
                time.sleep(self.config.sleep_time_sec)
                attempts += 1
        if terminated:
            log.info("%s job terminated", self.job_id)
            assert True

        if attempts == self.config.max_retry_attempts:
            log.info(
                "%s max attempts reached to wait for goal state", self.job_id
            )
            log.info("current_state:%s", state)
            assert False

    def wait_for_state(self, goal_state="SUCCEEDED", failed_state="FAILED"):
        """
        Waits for the job to reach a particular state
        :param goal_state: The state to reach
        :param failed_state: The failed state of the job
        """
        state = ""
        attempts = 0
        start = time.time()
        log.info("%s waiting for state %s", self.job_id, goal_state)
        state_transition_failure = False
        while attempts < self.config.max_retry_attempts:
            try:
                request = job.GetRequest(id=peloton.JobID(value=self.job_id))
                resp = self.client.job_svc.Get(
                    request,
                    metadata=self.client.jobmgr_metadata,
                    timeout=self.config.rpc_timeout_sec,
                )
                runtime = resp.jobInfo.runtime
                new_state = job.JobState.Name(runtime.state)
                if state != new_state:
                    log.info(
                        "%s transitioned to state %s", self.job_id, new_state
                    )
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
            task_failure = self._get_task_failure()
            log.info(
                "%s goal_state:%s current_state:%s attempts: %s",
                self.job_id,
                goal_state,
                state,
                str(attempts),
            )
            for t, failure in task_failure.iteritems():
                log.info(
                    "%s task id:%s failed for reason:%s message:%s",
                    self.job_id,
                    t,
                    failure["reason"],
                    failure["message"],
                )
            assert False

        if attempts == self.config.max_retry_attempts:
            log.info(
                "%s max attempts reached to wait for goal state", self.job_id
            )
            log.info(
                "%s goal_state:%s current_state:%s",
                self.job_id,
                goal_state,
                state,
            )
            assert False

        end = time.time()
        elapsed = end - start
        log.info("%s state transition took %s seconds", self.job_id, elapsed)
        assert state == goal_state

    def wait_for_condition(self, condition):
        """
        Waits for a particular condition to be met with the job
        :param condition: The condition to meet
        """
        wait_for_condition(
            message=self.job_id, condition=condition, config=self.config
        )

    def wait_for_jobmgr_available(self):
        """
        utility method to wait for job manger leader to come up.
        good practice to check before all write apis
        """
        attempts = 0
        while attempts < self.config.max_retry_attempts:
            try:
                request = job.DeleteRequest(
                    id=peloton.JobID(value="dummy_job_id")
                )
                self.client.job_svc.Delete(
                    request,
                    metadata=self.client.jobmgr_metadata,
                    timeout=self.config.rpc_timeout_sec,
                )
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    time.sleep(self.config.sleep_time_sec)
                    attempts += 1
                    continue
            break

    def update_instance_count(self, count):
        """
        Updates the instance count of a job
        :param count: The new count
        """
        self.job_config.instanceCount = count
        self.job_config.sla.maximumRunningInstances = count

    def _get_task_failure(self):
        """
        Returns a dict of task id to task failure reason and message.
        Useful when finding out the reason for job failure.
        :return: a dict of task id to failure dict
        {
          "5cd91f76-0846-44a5-b62f-d8c1054969dd" : {
                "reason" : TASK_LOST,
                "message": "task lost due to...",
          },
        }
        """
        task_id_reason_dict = {}
        for t in self.get_tasks().values():
            if t.state == task.FAILED:
                t_runtime = t.get_info().runtime
                task_id_reason_dict[t.instance_id] = {
                    "reason": t_runtime.reason,
                    "message": t_runtime.message,
                }
        return task_id_reason_dict


def query_jobs():
    """
    Query all batch jobs
    """
    client = Client()
    request = job.QueryRequest(summaryOnly=True)
    resp = client.job_svc.Query(
        request, metadata=client.jobmgr_metadata, timeout=10
    )

    jobs = []
    for j in resp.results:
        j = Job(job_id=j.id.value)
        jobs.append(j)
    return jobs


def kill_jobs(jobs):
    """
    Kills all the jobs
    :param jobs: The jobs to kill
    """
    for j in jobs:
        j.stop()

    for j in jobs:
        j.wait_for_terminated()

    # opportunistic delete for a batch job, once stopped
    # to prevent them from query jobs in next test execution
    for j in jobs:
        j.delete()


def get_active_jobs():
    """
    Gets all jobs from the active_jobs table
    """
    client.Client()
    request = job.GetActiveJobsRequest()
    resp = client.job_svc.GetActiveJobs(
        request, metadata=client.jobmgr_metadata, timeout=10
    )
    return resp.ids


def format_stats(stats):
    return " ".join(
        (
            "%s: %s" % (name.lower(), stats[name])
            for name in job.JobState.keys()
        )
    )
