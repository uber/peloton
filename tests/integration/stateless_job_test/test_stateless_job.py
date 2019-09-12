import logging
import grpc
import operator
import pytest
import time

from google.protobuf import json_format

from tests.integration.common import IntegrationTestConfig
from tests.integration.job import get_active_jobs
from tests.integration.conftest import get_container
from tests.integration.host import (
    get_host_in_state,
    wait_for_host_state,
    is_host_in_state,
    query_hosts,
)
from tests.integration.stateless_job import (
    INVALID_ENTITY_VERSION_ERR_MESSAGE,
    list_jobs,
    StatelessJob,
)
from tests.integration.stateless_job_test.util import (
    assert_pod_id_changed,
    assert_pod_id_equal,
    get_host_to_task_count,
)
from tests.integration.util import load_test_config

from peloton_client.pbgen.peloton.api.v0.task import task_pb2
from peloton_client.pbgen.peloton.api.v0.host import host_pb2
from peloton_client.pbgen.peloton.api.v1alpha.pod import pod_pb2

pytestmark = [
    pytest.mark.default,
    pytest.mark.stateless,
    pytest.mark.random_order(disabled=True),
]

log = logging.getLogger(__name__)


@pytest.mark.k8s
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


@pytest.mark.k8s
@pytest.mark.smoketest
def test__stop_stateless_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")
    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")


@pytest.mark.k8s
@pytest.mark.smoketest
def test__create_job_without_default_spec(stateless_job):
    default_spec = stateless_job.job_spec.default_spec
    stateless_job.job_spec.ClearField("default_spec")
    for i in range(0, stateless_job.job_spec.instance_count):
        stateless_job.job_spec.instance_spec[i].CopyFrom(default_spec)

    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")


@pytest.mark.k8s
@pytest.mark.smoketest
def test__stop_start_all_tasks_stateless_kills_tasks_and_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    stateless_job.stop()
    stateless_job.wait_for_state(goal_state="KILLED")

    stateless_job.start()
    stateless_job.wait_for_all_pods_running()


@pytest.mark.k8s
def test__exit_task_automatically_restart(stateless_job):
    stateless_job.config = IntegrationTestConfig(
        max_retry_attempts=100,
        pool_file='test_stateless_respool.yaml')
    stateless_job.set_command('sleep', args=['10'])
    stateless_job.create()
    stateless_job.wait_for_state(goal_state="RUNNING")

    old_pod_id = stateless_job.get_pod(0).get_pod_status().pod_id.value

    def job_not_running():
        return stateless_job.get_status().state != "JOB_STATE_RUNNING"

    stateless_job.wait_for_condition(job_not_running)

    def pod_id_changed():
        new_pod_id = stateless_job.get_pod(0).get_pod_status().pod_id.value
        return old_pod_id != new_pod_id

    stateless_job.wait_for_condition(pod_id_changed)


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


@pytest.mark.skip("issue with force deleting initialized job")
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
@pytest.mark.k8s
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


# Test pod kill due to host maintenance when SLA is not violated
def test__host_maintenance_within_sla_limit(stateless_job, maintenance):
    """
    1. Create a stateless job(instance_count=4) and MaximumUnavailableInstances=1.
       Wait for all pods to reach RUNNING state. This means there is at least one
       host with more than one instance.
    2. Start host maintenance on a host (say A) with more than 1 instance.
    3. Pods on the host A should get killed in a way (1 at a time)
       that doesn't violate the SLA and host A should transition to DOWN
    """
    stateless_job.job_spec.instance_count = 4
    stateless_job.job_spec.sla.maximum_unavailable_instances = 1
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    hosts = [h.hostname for h in query_hosts([]).host_infos]
    host_to_task_count = get_host_to_task_count(hosts, stateless_job)
    sorted_hosts = [t[0] for t in sorted(
        host_to_task_count.items(), key=operator.itemgetter(1))]

    # Pick a host that has pods running on it and start maintenance on it.
    test_host = sorted_hosts[0]

    resp = maintenance["start"]([test_host])
    assert resp

    # Wait for host to transition to DOWN
    attempts = 0
    max_retry_attempts = 20

    log.info(
        "%s waiting for state %s",
        test_host,
        host_pb2.HostState.Name(host_pb2.HOST_STATE_DOWN),
    )
    while attempts < max_retry_attempts:
        try:
            if is_host_in_state(test_host, host_pb2.HOST_STATE_DOWN):
                break

            # if the number of available pods is less than 2 (instance_count -
            # maximum_unavailable_instances) fail the test
            if len(stateless_job.query_pods(states=[pod_pb2.POD_STATE_RUNNING])) < 2:
                assert False
        except Exception as e:
            log.warn(e)
        finally:
            time.sleep(5)
            attempts += 1

    if attempts == max_retry_attempts:
        log.info(
            "%s max attempts reached to wait for host state %s",
            test_host,
            host_pb2.HostState.Name(host_pb2.HOST_STATE_DOWN),
        )
        assert False


# Test pod kill due to host maintenance which can violate job SLA
def test__host_maintenance_violate_sla(stateless_job, maintenance):
    """
    1. Create a stateless job(instance_count=4) with host-limit-1 constraint and
       MaximumUnavailableInstances=1. This means that there one instance that is
       unavailable.
    2. Start host maintenance on one of the hosts (say A).
    3. Since one instance is already unavailable, no more instances should be
       killed due to host maintenance. Verify that host A does not transition
       to DOWN.
    """
    job_spec_dump = load_test_config('test_stateless_job_spec_sla.yaml')
    json_format.ParseDict(job_spec_dump, stateless_job.job_spec)
    stateless_job.job_spec.instance_count = 4
    stateless_job.create()
    stateless_job.wait_for_all_pods_running(num_pods=3)

    # Pick a host that is UP and start maintenance on it
    test_host1 = get_host_in_state(host_pb2.HOST_STATE_UP)
    resp = maintenance["start"]([test_host1])
    assert resp

    try:
        wait_for_host_state(test_host1, host_pb2.HOST_STATE_DOWN)
        assert False, 'Host should not transition to DOWN'
    except:
        assert is_host_in_state(test_host1, host_pb2.HOST_STATE_DRAINING)
        assert len(stateless_job.query_pods(
            states=[pod_pb2.POD_STATE_RUNNING])) == 3


# Test killing a job whose SLA is violated
def test__kill_sla_violated_job():
    """
    1. Create a stateless job(instance_count=5) with host-limit-1 constraint and
       MaximumUnavailableInstances=1. Since there are only 3 UP hosts, 2 of
       the instances will not get placed (hence unavailable).
    2. Kill job and wait for the job to reach KILLED state
    """
    job = StatelessJob(
        job_file="test_stateless_job_spec_sla.yaml",
    )
    job.job_spec.instance_count = 5
    job.create()
    job.wait_for_all_pods_running(num_pods=3)

    job.stop()
    job.wait_for_state(goal_state='KILLED')


# Test deleting a job whose SLA is violated
def test__delete_sla_violated_job():
    """
    1. Create a stateless job(instance_count=5) with host-limit-1 constraint and
       MaximumUnavailableInstances=1. Since there are only 3 UP hosts, 2 of
       the instances will not get placed (hence unavailable).
    2. Force delete the job and verify that the job is deleted
    """
    job = StatelessJob(
        job_file="test_stateless_job_spec_sla.yaml",
    )
    job.job_spec.instance_count = 5
    job.create()
    job.wait_for_all_pods_running(num_pods=3)

    job.delete(force_delete=True)
    time.sleep(10)

    try:
        job.get_job()
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND
        return
    raise Exception("job not found error not received")


# Test restart job manager when waiting for pod kill due to
# host maintenance which can violate job SLA.
def test__host_maintenance_violate_sla_restart_jobmgr(stateless_job, maintenance, jobmgr):
    """
    1. Create a stateless job(instance_count=4) with host-limit-1 constraint and
       MaximumUnavailableInstances=1. Since there are only 3 UP hosts, one of
       the instances will not get placed (hence unavailable).
    2. Start host maintenance on one of the hosts (say A).
    3. Restart job manager.
    4. Since one instance is already unavailable, no more instances should be
       killed due to host maintenance. Verify that host A does not transition to DOWN
    """
    job_spec_dump = load_test_config('test_stateless_job_spec_sla.yaml')
    json_format.ParseDict(job_spec_dump, stateless_job.job_spec)
    stateless_job.job_spec.instance_count = 4
    stateless_job.create()
    stateless_job.wait_for_all_pods_running(num_pods=3)

    # Pick a host that is UP and start maintenance on it
    test_host1 = get_host_in_state(host_pb2.HOST_STATE_UP)
    resp = maintenance["start"]([test_host1])
    assert resp

    jobmgr.restart()

    try:
        wait_for_host_state(test_host1, host_pb2.HOST_STATE_DOWN)
        assert False, 'Host should not transition to DOWN'
    except:
        assert is_host_in_state(test_host1, host_pb2.HOST_STATE_DRAINING)
        assert len(stateless_job.query_pods(
            states=[pod_pb2.POD_STATE_RUNNING])) == 3


# Test pod stop/start for a job whose SLA is violated
def test__stop_start_pod_on_sla_violated_job(stateless_job):
    """
    1. Create a stateless job(instance_count=5) with host-limit-1 constraint and
       MaximumUnavailableInstances=1. Since there are only 3 UP hosts, 2 of
       the instances will not get placed (hence unavailable).
    2. Kill one of the running instances (say i). Instance should get killed.
    3. Start instance i. Instance i should transit to PENDING (due to host
       limit 1 constraint, instance won't get placed).
    """
    job_spec_dump = load_test_config('test_stateless_job_spec_sla.yaml')
    json_format.ParseDict(job_spec_dump, stateless_job.job_spec)
    stateless_job.job_spec.instance_count = 5
    stateless_job.create()
    stateless_job.wait_for_all_pods_running(num_pods=3)

    test_instance = None
    for i in range(0, stateless_job.job_spec.instance_count):
        if stateless_job.get_pod_status(i).state == pod_pb2.POD_STATE_RUNNING:
            test_instance = i
            break

    print test_instance
    assert not test_instance == None

    ranges = task_pb2.InstanceRange(to=test_instance+1)
    setattr(ranges, "from", test_instance)
    stateless_job.stop(ranges=[ranges])

    def instance_killed():
        return stateless_job.get_pod_status(test_instance).state == pod_pb2.POD_STATE_KILLED

    stateless_job.wait_for_condition(instance_killed)

    stateless_job.start(ranges=[ranges])

    def instance_pending():
        return stateless_job.get_pod_status(test_instance).state == pod_pb2.POD_STATE_PENDING

    stateless_job.wait_for_condition(instance_pending)


# Test host maintenance when job has no SLA defined
def test__host_maintenance_no_sla_defined_for_job(stateless_job, maintenance):
    """
    1. Create a stateless job(instance_count=3) without SLA defined
    2. Start host maintenance on all 3 hosts.
    3. The pods should get killed and all the hosts should transition to DOWN
    """
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    hosts = query_hosts([]).host_infos
    maintenance["start"]([h.hostname for h in hosts])

    for h in hosts:
        wait_for_host_state(h.hostname, host_pb2.HOST_STATE_DOWN)


def test__host_maintenance_and_agent_down(stateless_job, maintenance):
    """
    1. Create a large stateless job )that take up more than two-thirds of
       the cluster resources) with MaximumUnavailableInstances=2.
    2. Start host maintenance on one of the hosts (say A) having pods of the job.
       MaximumUnavailableInstances=2 ensures that not more than 2 pods are
       unavailable due to host maintenance at a time.
    3. Take down another host which has pods running on it. This will TASK_LOST
       to be sent for all pods on the host after 75 seconds.
    4. Since TASK_LOST would cause the job SLA to be violated (due to insufficient
       resources), instances on the host A should not be killed once LOST event
       is received. Verify that host A does not transition to DOWN.
    """
    stateless_job.job_spec.instance_count = 30
    stateless_job.job_spec.default_spec.containers[0].resource.cpu_limit = 0.3
    stateless_job.job_spec.sla.maximum_unavailable_instances = 2
    stateless_job.create()
    stateless_job.wait_for_all_pods_running()

    hosts = [h.hostname for h in query_hosts([]).host_infos]
    host_to_task_count = get_host_to_task_count(hosts, stateless_job)
    sorted_hosts = [t[0] for t in sorted(
        host_to_task_count.items(), key=operator.itemgetter(1), reverse=True)]

    # take down another host which has pods of the job
    host_container = get_container([sorted_hosts[1]])

    # Pick a host that has pods running on it and start maintenance on it.
    test_host = sorted_hosts[0]

    try:
        host_container.stop()
        maintenance["start"]([test_host])

        wait_for_host_state(test_host, host_pb2.HOST_STATE_DOWN)
        assert False, 'Host should not transition to DOWN'
    except:
        assert is_host_in_state(test_host, host_pb2.HOST_STATE_DRAINING)
        pass
    finally:
        host_container.start()
