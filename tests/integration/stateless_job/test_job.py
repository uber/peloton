import pytest
import time

from tests.integration.job import Job
from tests.integration.common import IntegrationTestConfig

pytestmark = [pytest.mark.default, pytest.mark.stateless]


@pytest.mark.smoketest
def test__create_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')


@pytest.mark.smoketest
def test__stop_stateless_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    stateless_job.stop()
    stateless_job.wait_for_state(goal_state='KILLED')


@pytest.mark.smoketest
def test__stop_start_all_tasks_stateless_kills_tasks_and_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state='KILLED')

    stateless_job.start()
    stateless_job.wait_for_state(goal_state='RUNNING')


def test__exit_task_automatically_restart():
    job = Job(job_file='test_stateless_job_exit_without_err.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    old_mesos_id = job.get_task(0).get_info().runtime.mesosTaskId.value

    def job_not_running():
        return job.get_runtime().state != 'RUNNING'

    job.wait_for_condition(job_not_running)

    def task_mesos_id_changed():
        new_mesos_id = job.get_task(0).get_info().runtime.mesosTaskId.value
        return old_mesos_id != new_mesos_id

    job.wait_for_condition(task_mesos_id_changed)


def test__failed_task_automatically_restart():
    job = Job(job_file='test_stateless_job_exit_with_err.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    old_mesos_id = job.get_task(0).get_info().runtime.mesosTaskId.value

    def job_not_running():
        return job.get_runtime().state != 'RUNNING'

    job.wait_for_condition(job_not_running)

    def task_mesos_id_changed():
        new_mesos_id = job.get_task(0).get_info().runtime.mesosTaskId.value
        return old_mesos_id != new_mesos_id

    job.wait_for_condition(task_mesos_id_changed)


def test__health_check_detects_unhealthy_tasks():
    job = Job(job_file='test_stateless_job_failed_health_check_config.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.job_config.instanceCount = 1
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    def task_has_unhealthy_events():
        for pod_event in job.get_pod_events(0):
            if pod_event.healthy == 'UNHEALTHY':
                return True

    job.wait_for_condition(task_has_unhealthy_events)


def test__health_check_detects_healthy_tasks():
    job = Job(job_file='test_stateless_job_successful_health_check_config.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.job_config.instanceCount = 1
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    def task_has_healthy_events():
        for pod_event in job.get_pod_events(0):
            if pod_event.healthy == 'HEALTHY':
                return True

    job.wait_for_condition(task_has_healthy_events)


def test__failed_task_throttled_by_exponential_backoff():
    job = Job(job_file='test_stateless_job_exit_with_err.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    time.sleep(40)

    pod_events = job.get_pod_events(0)
    # if throttle is effective, the task should not create many
    # pod events. Otherwise it can generate many pod events, during
    # the time window
    assert 1 < len(pod_events) < 20
