import grpc
import pytest
import time

from tests.integration.stateless_job import StatelessJob
from tests.integration.common import IntegrationTestConfig
from tests.integration.stateless_job import INVALID_ENTITY_VERSION_ERR_MESSAGE

pytestmark = [pytest.mark.default, pytest.mark.stateless]


@pytest.mark.smoketest
def test__create_job(stateless_job_v1alpha):
    stateless_job_v1alpha.create()
    stateless_job_v1alpha.wait_for_state(goal_state='RUNNING')


@pytest.mark.smoketest
def test__stop_stateless_job(stateless_job_v1alpha):
    stateless_job_v1alpha.create()
    stateless_job_v1alpha.wait_for_state(goal_state='RUNNING')
    stateless_job_v1alpha.stop()
    stateless_job_v1alpha.wait_for_state(goal_state='KILLED')


@pytest.mark.smoketest
def test__stop_start_all_tasks_stateless_kills_tasks_and_job(stateless_job_v1alpha):
    stateless_job_v1alpha.create()
    stateless_job_v1alpha.wait_for_state(goal_state='RUNNING')

    stateless_job_v1alpha.stop()
    stateless_job_v1alpha.wait_for_state(goal_state='KILLED')

    stateless_job_v1alpha.start()
    stateless_job_v1alpha.wait_for_state(goal_state='RUNNING')


@pytest.mark.smoketest
def test__create_job_without_default_spec(stateless_job_v1alpha):
    default_spec = stateless_job_v1alpha.job_spec.default_spec
    stateless_job_v1alpha.job_spec.ClearField('default_spec')
    for i in range(0, stateless_job_v1alpha.job_spec.instance_count):
        stateless_job_v1alpha.job_spec.instance_spec[i].CopyFrom(default_spec)

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


def test__start_job_in_killing_state(stateless_job_v1alpha, mesos_master):
    stateless_job_v1alpha.create()
    stateless_job_v1alpha.wait_for_state(goal_state='RUNNING')

    # stop mesos master to ensure job doesn't transition to KILLED
    mesos_master.stop()
    stateless_job_v1alpha.stop()
    stateless_job_v1alpha.wait_for_state(goal_state='KILLING')

    stateless_job_v1alpha.start()
    mesos_master.start()
    stateless_job_v1alpha.wait_for_state(goal_state='RUNNING')


def test__start_job_in_initialized_state(stateless_job_v1alpha):
    stateless_job_v1alpha.create()
    stateless_job_v1alpha.wait_for_state(goal_state='INITIALIZED')

    # it is possible that the job might have transitioned to PENDING
    # since there is no way to ensure this transition doesn't happen
    stateless_job_v1alpha.start()
    stateless_job_v1alpha.wait_for_state(goal_state='RUNNING')


def test__start_job_bad_version(stateless_job_v1alpha):
    stateless_job_v1alpha.create()

    try:
        stateless_job_v1alpha.start(entity_version='1-2-3')
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert e.details() == INVALID_ENTITY_VERSION_ERR_MESSAGE
        return
    raise Exception("entity version mismatch error not received")


def test__stop_job_bad_version(stateless_job_v1alpha):
    stateless_job_v1alpha.create()
    stateless_job_v1alpha.wait_for_state(goal_state='RUNNING')

    try:
        stateless_job_v1alpha.stop(entity_version='1-2-3')
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert e.details() == INVALID_ENTITY_VERSION_ERR_MESSAGE
        return
    raise Exception("entity version mismatch error not received")
