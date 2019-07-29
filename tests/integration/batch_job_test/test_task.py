import grpc
import pytest
import time

from tests.integration.job import (
    IntegrationTestConfig,
    Job,
    kill_jobs,
    with_instance_count,
)
from peloton_client.pbgen.peloton.api.v0.task import task_pb2
from tests.integration.client import Client, with_private_stubs


pytestmark = [
    pytest.mark.default,
    pytest.mark.task,
    pytest.mark.random_order(disabled=True),
]


@pytest.mark.smoketest
def test__stop_start_all_tasks_kills_tasks_and_job(long_running_job):
    long_running_job.create()
    long_running_job.wait_for_state(goal_state="RUNNING")

    long_running_job.stop()
    long_running_job.wait_for_state(goal_state="KILLED")

    try:
        long_running_job.start()
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert e.details() == "cannot start tasks in a terminated job"
        return
    raise Exception("was able to start terminated job")


def test__stop_start_partial_tests_with_single_range(long_running_job):
    long_running_job.create()
    long_running_job.wait_for_state(goal_state="RUNNING")

    range = task_pb2.InstanceRange(to=1)
    setattr(range, "from", 0)

    def wait_for_instance_to_stop():
        return long_running_job.get_task(0).state_str == "KILLED"

    long_running_job.stop(ranges=[range])
    long_running_job.wait_for_condition(wait_for_instance_to_stop)

    def wait_for_instance_to_run():
        return long_running_job.get_task(0).state_str == "RUNNING"

    long_running_job.start(ranges=[range])
    long_running_job.wait_for_condition(wait_for_instance_to_run)

    long_running_job.stop()
    long_running_job.wait_for_state(goal_state="KILLED")


def test__stop_start_partial_tests_with_multiple_ranges(long_running_job):
    long_running_job.create()
    long_running_job.wait_for_state(goal_state="RUNNING")

    range1 = task_pb2.InstanceRange(to=1)
    setattr(range1, "from", 0)
    range2 = task_pb2.InstanceRange(to=2)
    setattr(range2, "from", 1)

    def wait_for_instance_to_stop():
        return (
            long_running_job.get_task(0).state_str == "KILLED"
            and long_running_job.get_task(1).state_str == "KILLED"
        )

    long_running_job.stop(ranges=[range1, range2])
    long_running_job.wait_for_condition(wait_for_instance_to_stop)

    def wait_for_instance_to_run():
        return (
            long_running_job.get_task(0).state_str == "RUNNING"
            and long_running_job.get_task(1).state_str == "RUNNING"
        )

    long_running_job.start(ranges=[range1, range2])
    long_running_job.wait_for_condition(wait_for_instance_to_run)

    long_running_job.stop()
    long_running_job.wait_for_state(goal_state="KILLED")


def test__start_stop_task_without_job_id():
    job_without_id = Job()
    resp = job_without_id.start()
    assert resp.HasField("error")
    assert resp.error.HasField("notFound")

    resp = job_without_id.stop()
    assert resp.HasField("error")
    assert resp.error.HasField("notFound")


def test__start_stop_task_with_nonexistent_job_id():
    job_with_nonexistent_id = Job()
    job_with_nonexistent_id.job_id = "nonexistent-job-id"
    resp = job_with_nonexistent_id.start()
    assert resp.HasField("error")
    assert resp.error.HasField("notFound")

    resp = job_with_nonexistent_id.stop()
    assert resp.HasField("error")
    assert resp.error.HasField("notFound")


def test__stop_start_tasks_when_mesos_master_down_kills_tasks_when_started(
    long_running_job, mesos_master
):
    long_running_job.create()
    long_running_job.wait_for_state(goal_state="RUNNING")

    range = task_pb2.InstanceRange(to=1)
    setattr(range, "from", 0)

    def wait_for_instance_to_stop():
        return long_running_job.get_task(0).state_str == "KILLED"

    mesos_master.stop()
    long_running_job.stop(ranges=[range])
    mesos_master.start()
    long_running_job.wait_for_condition(wait_for_instance_to_stop)

    def wait_for_instance_to_run():
        return long_running_job.get_task(0).state_str == "RUNNING"

    mesos_master.stop()
    long_running_job.start(ranges=[range])
    mesos_master.start()
    long_running_job.wait_for_condition(wait_for_instance_to_run)

    mesos_master.stop()
    long_running_job.stop()
    mesos_master.start()
    long_running_job.wait_for_terminated()


def test__stop_start_tasks_when_mesos_master_down_and_jobmgr_restarts(
    long_running_job, mesos_master, jobmgr
):
    long_running_job.create()
    long_running_job.wait_for_state(goal_state="RUNNING")

    range = task_pb2.InstanceRange(to=1)
    setattr(range, "from", 0)

    def wait_for_instance_to_stop():
        return long_running_job.get_task(0).state_str == "KILLED"

    mesos_master.stop()
    long_running_job.stop(ranges=[range])
    jobmgr.restart()
    mesos_master.start()
    long_running_job.wait_for_condition(wait_for_instance_to_stop)

    def wait_for_instance_to_run():
        return long_running_job.get_task(0).state_str == "RUNNING"

    mesos_master.stop()
    long_running_job.start(ranges=[range])
    jobmgr.restart()
    mesos_master.start()
    long_running_job.wait_for_condition(wait_for_instance_to_run)

    mesos_master.stop()
    long_running_job.stop()
    jobmgr.restart()
    mesos_master.start()
    long_running_job.wait_for_terminated()


def test__kill_mesos_agent_makes_task_resume(long_running_job, mesos_agent):
    long_running_job.job_config.defaultConfig.restartPolicy.maxFailures = 1
    long_running_job.create()
    long_running_job.wait_for_state(goal_state="RUNNING")

    mesos_agent.restart()

    long_running_job.wait_for_state(goal_state="RUNNING")


def test_controller_task_limit():
    # This tests the controller limit of a resource pool. Once it is fully
    # allocated by a controller task, subsequent tasks can't be admitted.
    # 1. start controller job1 which uses all the controller limit
    # 2. start controller job2, make sure it remains pending.
    # 3. kill  job1, make sure job2 starts running.

    # job1 uses all the controller limit
    job1 = Job(
        job_file="test_controller_job.yaml",
        config=IntegrationTestConfig(
            pool_file="test_respool_controller_limit.yaml"
        ),
    )

    job1.create()
    job1.wait_for_state(goal_state="RUNNING")

    # job2 should remain pending as job1 used the controller limit
    job2 = Job(
        job_file="test_controller_job.yaml",
        config=IntegrationTestConfig(
            pool_file="test_respool_controller_limit.yaml"
        ),
    )
    job2.create()

    # sleep for 5 seconds to make sure job 2 has enough time
    time.sleep(5)

    # make sure job2 can't run
    job2.wait_for_state(goal_state="PENDING")

    # stop job1
    job1.stop()
    job1.wait_for_state(goal_state="KILLED")

    # make sure job2 starts running
    job2.wait_for_state(goal_state="RUNNING")

    kill_jobs([job2])


def test_controller_task_limit_executor_can_run():
    # This tests the controller limit isn't applied to non-controller jobs.
    # 1. start controller cjob1 which uses all the controller limit
    # 2. start controller cjob2, make sure it remains pending.
    # 3. start non-controller job, make sure it succeeds.

    # job1 uses all the controller limit
    cjob1 = Job(
        job_file="test_controller_job.yaml",
        config=IntegrationTestConfig(
            pool_file="test_respool_controller_limit.yaml"
        ),
    )

    cjob1.create()
    cjob1.wait_for_state(goal_state="RUNNING")

    # job2 should remain pending as job1 used the controller limit
    cjob2 = Job(
        job_file="test_controller_job.yaml",
        config=IntegrationTestConfig(
            pool_file="test_respool_controller_limit.yaml"
        ),
    )
    cjob2.create()

    # sleep for 5 seconds to make sure job 2 has enough time
    time.sleep(5)

    # make sure job2 can't run
    cjob2.wait_for_state(goal_state="PENDING")

    # start a normal executor job
    job = Job(
        job_file="test_job.yaml",
        config=IntegrationTestConfig(
            pool_file="test_respool_controller_limit.yaml"
        ),
    )
    job.create()

    # make sure job can run and finish
    job.wait_for_state(goal_state="SUCCEEDED")

    kill_jobs([cjob1, cjob2])


def test_job_succeeds_if_controller_task_succeeds():
    # only controller task in cjob would succeed.
    # other tasks would fail, but only controller task should determine
    # job terminal state
    cjob = Job(job_file="test_job_succecced_controller_task.yaml")

    cjob.create()
    cjob.wait_for_state(goal_state="SUCCEEDED")

    kill_jobs([cjob])


def test_task_killed_in_ready_succeeds_when_re_enqueued(placement_engines):
    # Tests that a if task is deleted which is in READY state in resource
    # manager and if is re-enqueued succeeds.

    # stop the placement engines to keep the tasks in READY state
    placement_engines.stop()

    # decorate the client to add peloton private API stubs
    c = with_private_stubs(Client())

    # create long running job with 2 instances
    long_running_job = Job(
        job_file="long_running_job.yaml",
        options=[with_instance_count(2)],
        client=c,
    )

    long_running_job.create()
    long_running_job.wait_for_state(goal_state="PENDING")

    task = long_running_job.get_task(0)
    # wait for task to reach READY
    task.wait_for_pending_state(goal_state="READY")

    # kill the task
    task.stop()

    # re-enqueue the task
    task.start()

    # gentlemen, start your (placement) engines
    placement_engines.start()

    def wait_for_instance_to_run():
        return long_running_job.get_task(0).state_str == "RUNNING"

    long_running_job.wait_for_condition(wait_for_instance_to_run)
