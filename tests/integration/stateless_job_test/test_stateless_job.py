import grpc
import pytest
import time

from tests.integration.stateless_job import StatelessJob
from tests.integration.common import IntegrationTestConfig
from tests.integration.job import get_active_jobs
from tests.integration.stateless_job import (
    INVALID_ENTITY_VERSION_ERR_MESSAGE,
    list_jobs,
)
from tests.integration.stateless_job_test.util import (
    assert_pod_id_changed,
    assert_pod_id_equal,
)

from peloton_client.pbgen.peloton.api.v0.task import task_pb2
from peloton_client.pbgen.peloton.api.v1alpha.pod import pod_pb2

pytestmark = [
    pytest.mark.default,
    pytest.mark.stateless,
    pytest.mark.random_order(disabled=True),
]


@pytest.mark.smoketest
def test__create_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    # ensure ListJobs lists the job
    jobSummaries = list_jobs()
    assert len(jobSummaries) > 0
    statelessJobSummary = None
    for jobSummary in jobSummaries:
        if jobSummary.job_id.value == stateless_job.job_id:
            statelessJobSummary = jobSummary
            break
    assert statelessJobSummary is not None

    # ensure ListPods lists all the pods of the job
    podSummaries = stateless_job.list_pods()
    assert len(podSummaries) == statelessJobSummary.instance_count


@pytest.mark.smoketest
def test__stop_stateless_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")


@pytest.mark.smoketest
def test__create_job_without_default_spec(stateless_job):
    default_spec = stateless_job.job_spec.default_spec
    stateless_job.job_spec.ClearField("default_spec")
    for i in range(0, stateless_job.job_spec.instance_count):
        stateless_job.job_spec.instance_spec[i].CopyFrom(default_spec)

    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")


@pytest.mark.smoketest
def test__stop_start_all_tasks_stateless_kills_tasks_and_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    stateless_job.start()
    stateless_job.wait_for_all_pods_running()


def test__exit_task_automatically_restart():
    job = StatelessJob(
        job_file="test_stateless_job_exit_without_err_spec.yaml",
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
    )
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    old_pod_id = job.get_pod(0).get_pod_status().pod_id.value

    def job_not_running():
        return job.get_status().state != "JOB_STATE_RUNNING"

    job.wait_for_condition(job_not_running)

    def pod_id_changed():
        new_pod_id = job.get_pod(0).get_pod_status().pod_id.value
        return old_pod_id != new_pod_id

    job.wait_for_condition(pod_id_changed)


def test__failed_task_automatically_restart():
    job = StatelessJob(
        job_file="test_stateless_job_exit_with_err_spec.yaml",
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
    )
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    old_pod_id = job.get_pod(0).get_pod_status().pod_id.value

    def job_not_running():
        return job.get_status().state != "JOB_STATE_RUNNING"

    job.wait_for_condition(job_not_running)

    def pod_id_changed():
        new_pod_id = job.get_pod(0).get_pod_status().pod_id.value
        return old_pod_id != new_pod_id

    job.wait_for_condition(pod_id_changed)


def test__health_check_detects_unhealthy_tasks():
    job = StatelessJob(
        job_file="test_stateless_job_failed_health_check_spec.yaml",
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
    )
    job.job_spec.instance_count = 1
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    def task_has_unhealthy_events():
        for pod_event in job.get_pod(0).get_pod_events():
            if pod_event.healthy == "HEALTH_STATE_UNHEALTHY":
                return True

    job.wait_for_condition(task_has_unhealthy_events)


def test__health_check_detects_healthy_tasks():
    job = StatelessJob(
        job_file="test_stateless_job_successful_health_check_spec.yaml",
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
    )
    job.job_spec.instance_count = 1
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    def task_has_healthy_events():
        for pod_event in job.get_pod(0).get_pod_events():
            if pod_event.healthy == "HEALTH_STATE_HEALTHY":
                return True

    job.wait_for_condition(task_has_healthy_events)


def test__failed_task_throttled_by_exponential_backoff():
    job = StatelessJob(
        job_file="test_stateless_job_exit_with_err_spec.yaml",
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            pool_file='test_stateless_respool.yaml',
        ),
    )
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    time.sleep(40)

    pod_events = job.get_pod(0).get_pod_events()
    # if throttle is effective, the task should not create many
    # pod events. Otherwise it can generate many pod events, during
    # the time window
    pod_id = pod_events[0].pod_id.value
    run_id = int(pod_id[pod_id.rindex("-") + 1:])
    assert 1 < run_id < 20


def test__start_job_in_killing_state(stateless_job, mesos_master):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    # stop mesos master to ensure job doesn't transition to KILLED
    mesos_master.stop()
    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLING")

    stateless_job.start()
    mesos_master.start()
    stateless_job.wait_for_all_pods_running()


def test__start_job_in_initialized_state(stateless_job):
    stateless_job.create()

    # it is possible that the job might have transitioned to INITIALIZED/PENDING
    # since there is no way to fine control the transitions
    stateless_job.start()
    stateless_job.wait_for_all_pods_running()


def test__start_job_bad_version(stateless_job):
    stateless_job.create()

    try:
        stateless_job.start(entity_version="1-2-3")
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ABORTED
        assert INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
        return
    raise Exception("entity version mismatch error not received")


def test__stop_job_bad_version(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    try:
        stateless_job.stop(entity_version="1-2-3")
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ABORTED
        assert INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
        return
    raise Exception("entity version mismatch error not received")


# test__delete_killed_job tests deleting a killed job
def test__delete_killed_job():
    job = StatelessJob()
    job.create()
    job.wait_for_state(goal_state="RUNNING")
    job_id = job.get_job_id()

    job.stop()
    job.wait_for_state(goal_state="KILLED")

    job.delete()
    time.sleep(10)

    try:
        job.get_job()
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND
        return
    raise Exception("job not found error not received")

    # try to find the job from active_jobs
    ids = get_active_jobs()
    assert job_id not in ids


# test__delete_running_job_with_force_flag tests force deleting a running job
def test__delete_running_job_with_force_flag():
    job = StatelessJob()
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    job.delete(force_delete=True)
    time.sleep(10)

    try:
        job.get_job()
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND
        return
    raise Exception("job not found error not received")


# test__delete_running_job_without_force_flag tests
# deleting a running job (without force flag set)
def test__delete_running_job_without_force_flag():
    job = StatelessJob()
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    try:
        job.delete()
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ABORTED
        return
    raise Exception("job in non-terminal state error not received")


# test__delete_initialized_job_with_force_flag tests
# deleting a INITIALIZED/PENDING job
def test__delete_initialized_job_with_force_flag():
    job = StatelessJob()
    job.create()
    # the job might have transitioned to INITIALIZED/PENDING
    # since there is no way to fine control the job transitions
    job.delete(force_delete=True)
    time.sleep(10)

    try:
        job.get_job()
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND
        return
    raise Exception("job not found error not received")


# test__delete_job_bad_version tests the failure case of deleting
# a job due to incorrect entity version provided in the request
def test__delete_job_bad_version():
    job = StatelessJob()
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    job.stop()
    job.wait_for_state(goal_state="KILLED")

    try:
        job.delete(entity_version="1-2-3")
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ABORTED
        assert INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
        return
    raise Exception("entity version mismatch error not received")


# test starting an already running job. Should be a noop
def test__start_running_job(stateless_job):
    stateless_job.create()

    stateless_job.wait_for_all_pods_running()
    old_pod_infos = stateless_job.query_pods()

    stateless_job.start()
    new_pod_infos = stateless_job.query_pods()
    # start should be a noop for already running instances
    assert_pod_id_equal(old_pod_infos, new_pod_infos)


# test starting a killed job
def test__start_killed_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    stateless_job.start()
    stateless_job.wait_for_all_pods_running()

    new_pod_infos = stateless_job.query_pods()
    assert_pod_id_changed(old_pod_infos, new_pod_infos)


# test start succeeds during jobmgr restart
def test__start_restart_jobmgr(stateless_job, jobmgr):
    stateless_job.create()
    # TODO: remove this line after update and kill race
    # condition is fixed
    stateless_job.wait_for_all_pods_running()
    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    stateless_job.start()
    jobmgr.restart()

    stateless_job.wait_for_all_pods_running()


# test stopping a job during jobmgr restart
def test__stop_restart_jobmgr(stateless_job, jobmgr):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    stateless_job.stop()

    jobmgr.restart()

    stateless_job.wait_for_state(goal_state="KILLED")


@pytest.mark.skip(reason="flaky test")
# test restarting running job, with batch size,
def test__restart_running_job_with_batch_size(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()

    stateless_job.restart(batch_size=1, in_place=in_place)
    stateless_job.wait_for_workflow_state(goal_state="SUCCEEDED")

    stateless_job.wait_for_all_pods_running()

    new_pod_infos = stateless_job.query_pods()
    # restart should kill and start already running instances
    assert_pod_id_changed(old_pod_infos, new_pod_infos)


# test restarting a killed job with batch size,
def test__restart_killed_job_with_batch_size(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")
    old_pod_infos = stateless_job.query_pods()

    # TODO add back batch size after API update in peloton client
    # stateless_job.restart(batch_size=1)
    stateless_job.restart(in_place=in_place)

    stateless_job.wait_for_all_pods_running()

    new_pod_infos = stateless_job.query_pods()
    assert_pod_id_changed(old_pod_infos, new_pod_infos)


# test restarting running job without batch size,
def test__restart_running_job(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()

    stateless_job.restart(in_place=in_place)
    stateless_job.wait_for_workflow_state(goal_state="SUCCEEDED")

    stateless_job.wait_for_all_pods_running()

    new_pod_infos = stateless_job.query_pods()
    # restart should kill and start already running instances
    assert_pod_id_changed(old_pod_infos, new_pod_infos)


# test restarting killed job without batch size,
def test__restart_killed_job(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    stateless_job.restart(in_place=in_place)

    stateless_job.wait_for_all_pods_running()

    new_pod_infos = stateless_job.query_pods()
    assert_pod_id_changed(old_pod_infos, new_pod_infos)


# test restarting a partial set of tasks of the job
def test__restart_partial_job(stateless_job, in_place):
    ranges = [pod_pb2.InstanceIDRange(to=2)]
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    old_pod_infos = stateless_job.query_pods()

    old_pod_dict = {}
    for old_pod_info in old_pod_infos:
        split_index = old_pod_info.status.pod_id.value.rfind("-")
        pod_name = old_pod_info.status.pod_id.value[:split_index]
        old_pod_dict[pod_name] = old_pod_info.status.pod_id.value
        assert old_pod_info.status.state == pod_pb2.POD_STATE_RUNNING

    stateless_job.restart(batch_size=1, ranges=ranges, in_place=in_place)
    stateless_job.wait_for_workflow_state(goal_state="SUCCEEDED")

    stateless_job.wait_for_all_pods_running()
    new_pod_infos = stateless_job.query_pods()
    for new_pod_info in new_pod_infos:
        new_pod_id = new_pod_info.status.pod_id.value

        split_index = new_pod_info.status.pod_id.value.rfind("-")
        pod_name = new_pod_info.status.pod_id.value[:split_index]
        old_pod_id = old_pod_dict[pod_name]
        # only pods in range [0-2) are restarted
        if int(pod_name[pod_name.rfind("-") + 1:]) < 2:
            assert old_pod_id != new_pod_id
        else:
            assert old_pod_id == new_pod_id


# test restarting job during jobmgr restart
def test__restart_restart_jobmgr(stateless_job, jobmgr, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = stateless_job.query_pods()
    stateless_job.restart(in_place=in_place)

    jobmgr.restart()
    stateless_job.wait_for_workflow_state(goal_state="SUCCEEDED")

    stateless_job.wait_for_all_pods_running()

    new_pod_infos = stateless_job.query_pods()
    assert_pod_id_changed(old_pod_infos, new_pod_infos)


# test restarting a job with wrong entity version
def test__restart_bad_version(stateless_job, in_place):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    try:
        stateless_job.restart(entity_version="1-2-3", in_place=in_place)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ABORTED
        return
    raise Exception("configuration version mismatch error not received")


def test__stop_start_tasks_when_mesos_master_down_kills_tasks_when_started(
        stateless_job, mesos_master
):
    """
    1. Create stateless job.
    2. Wait for job state RUNNING.
    3. Stop a subset of job instances when mesos master is down.
    4. Start mesos master and wait for the instances to be stopped.
    5. Start the same subset of instances when mesos master is down.
    6. Start mesos master and wait for the instances to transit to RUNNING.
    7. Stop the job when mesos master is down.
    8. Start mesos master and wait for the job to terminate
    """
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    range = task_pb2.InstanceRange(to=1)
    setattr(range, "from", 0)

    def wait_for_instance_to_stop():
        return stateless_job.get_task(0).state_str == "KILLED"

    mesos_master.stop()
    stateless_job.stop(ranges=[range])
    mesos_master.start()
    stateless_job.wait_for_condition(wait_for_instance_to_stop)

    def wait_for_instance_to_run():
        return stateless_job.get_task(0).state_str == "RUNNING"

    mesos_master.stop()
    stateless_job.start(ranges=[range])
    mesos_master.start()
    stateless_job.wait_for_condition(wait_for_instance_to_run)

    mesos_master.stop()
    stateless_job.stop()
    mesos_master.start()
    stateless_job.wait_for_terminated()


def test__stop_start_tasks_when_mesos_master_down_and_jobmgr_restarts(
        stateless_job, mesos_master, jobmgr
):
    """
    1. Create stateless job.
    2. Wait for job state RUNNING.
    3. Stop a subset of job instances when mesos master is down.
    4. Restart job manager.
    5. Start mesos master and wait for the instances to be stopped.
    6. Start the same subset of instances when mesos master is down.
    7. Restart job manager.
    8. Start mesos master and wait for the instances to transit to RUNNING.
    9. Stop the job when mesos master is down.
    10. Restart job manager.
    11. Start mesos master and wait for the job to terminate
    """
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    range = task_pb2.InstanceRange(to=1)
    setattr(range, "from", 0)

    def wait_for_instance_to_stop():
        return stateless_job.get_task(0).state_str == "KILLED"

    mesos_master.stop()
    stateless_job.stop(ranges=[range])
    jobmgr.restart()
    mesos_master.start()
    stateless_job.wait_for_condition(wait_for_instance_to_stop)

    def wait_for_instance_to_run():
        return stateless_job.get_task(0).state_str == "RUNNING"

    mesos_master.stop()
    stateless_job.start(ranges=[range])
    jobmgr.restart()
    mesos_master.start()
    stateless_job.wait_for_condition(wait_for_instance_to_run)

    mesos_master.stop()
    stateless_job.stop()
    jobmgr.restart()
    mesos_master.start()
    stateless_job.wait_for_terminated()


def test__kill_mesos_agent_makes_task_resume(stateless_job, mesos_agent):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    mesos_agent.restart()

    stateless_job.wait_for_state(goal_state="RUNNING")


def test__stop_start_partial_tests_with_single_range(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    range = task_pb2.InstanceRange(to=1)
    setattr(range, "from", 0)

    def wait_for_instance_to_stop():
        return stateless_job.get_task(0).state_str == "KILLED"

    stateless_job.stop(ranges=[range])
    stateless_job.wait_for_condition(wait_for_instance_to_stop)

    def wait_for_instance_to_run():
        return stateless_job.get_task(0).state_str == "RUNNING"

    stateless_job.start(ranges=[range])
    stateless_job.wait_for_condition(wait_for_instance_to_run)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")


def test__stop_start_partial_tests_with_multiple_ranges(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    range1 = task_pb2.InstanceRange(to=1)
    setattr(range1, "from", 0)
    range2 = task_pb2.InstanceRange(to=2)
    setattr(range2, "from", 1)

    def wait_for_instance_to_stop():
        return (
            stateless_job.get_task(0).state_str == "KILLED"
            and stateless_job.get_task(1).state_str == "KILLED"
        )

    stateless_job.stop(ranges=[range1, range2])
    stateless_job.wait_for_condition(wait_for_instance_to_stop)

    def wait_for_instance_to_run():
        return (
            stateless_job.get_task(0).state_str == "RUNNING"
            and stateless_job.get_task(1).state_str == "RUNNING"
        )

    stateless_job.start(ranges=[range1, range2])
    stateless_job.wait_for_condition(wait_for_instance_to_run)

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")
