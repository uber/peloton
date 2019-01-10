import pytest
import time

from tests.integration.stateless_job import StatelessJob
from tests.integration.common import IntegrationTestConfig

pytestmark = [pytest.mark.default, pytest.mark.stateless]


@pytest.mark.smoketest
def test__create_job(stateless_job_v1alpha):
    stateless_job_v1alpha.create()
    stateless_job_v1alpha.wait_for_state(goal_state='RUNNING')


def test__exit_task_automatically_restart():
    job = StatelessJob(
        job_file='test_stateless_job_exit_without_err_spec.yaml',
        config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    old_pod_id = job.get_pod(0).get_pod_status().pod_id.value

    def job_not_running():
        return job.get_status().state != 'JOB_STATE_RUNNING'

    job.wait_for_condition(job_not_running)

    def pod_id_changed():
        new_pod_id = job.get_pod(0).get_pod_status().pod_id.value
        return old_pod_id != new_pod_id

    job.wait_for_condition(pod_id_changed)


def test__failed_task_automatically_restart():
    job = StatelessJob(
        job_file='test_stateless_job_exit_with_err_spec.yaml',
        config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    old_pod_id = job.get_pod(0).get_pod_status().pod_id.value

    def job_not_running():
        return job.get_status().state != 'JOB_STATE_RUNNING'

    job.wait_for_condition(job_not_running)

    def pod_id_changed():
        new_pod_id = job.get_pod(0).get_pod_status().pod_id.value
        return old_pod_id != new_pod_id

    job.wait_for_condition(pod_id_changed)


def test__health_check_detects_unhealthy_tasks():
    job = StatelessJob(
        job_file='test_stateless_job_failed_health_check_spec.yaml',
        config=IntegrationTestConfig(max_retry_attempts=100))
    job.job_spec.instance_count = 1
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    def task_has_unhealthy_events():
        for pod_event in job.get_pod(0).get_pod_events():
            if pod_event.healthy == 'UNHEALTHY':
                return True

    job.wait_for_condition(task_has_unhealthy_events)


def test__health_check_detects_healthy_tasks():
    job = StatelessJob(
        job_file='test_stateless_job_successful_health_check_spec.yaml',
        config=IntegrationTestConfig(max_retry_attempts=100))
    job.job_spec.instance_count = 1
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    def task_has_healthy_events():
        for pod_event in job.get_pod(0).get_pod_events():
            if pod_event.healthy == 'HEALTHY':
                return True

    job.wait_for_condition(task_has_healthy_events)


def test__failed_task_throttled_by_exponential_backoff():
    job = StatelessJob(
        job_file='test_stateless_job_exit_with_err_spec.yaml',
        config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    time.sleep(40)

    pod_events = job.get_pod(0).get_pod_events()
    # if throttle is effective, the task should not create many
    # pod events. Otherwise it can generate many pod events, during
    # the time window
    pod_id = pod_events[0].pod_id.value
    run_id = int(pod_id[pod_id.rindex('-')+1:])
    assert 1 < run_id < 20
