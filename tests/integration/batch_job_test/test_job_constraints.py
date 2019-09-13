import pytest

from tests.integration.job import (
    IntegrationTestConfig,
    Job,
    with_constraint,
    with_instance_count,
)
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2
from peloton_client.pbgen.peloton.api.v0.task import task_pb2

# Mark test module so that we can run tests by tags
pytestmark = [
    pytest.mark.default,
    pytest.mark.job,
    pytest.mark.random_order(disabled=True),
]


# Start a job with 4 instances with hostlimit:1
# Since we have 3 mesos agents , 3 tasks should start running on different
# hosts and have 1 task PENDING since there's no other host to satisfy the
# constraint.
def test__host_limit(peloton_client):
    job = Job(
        client=peloton_client,
        job_file="test_stateless_job_host_limit_1.yaml",
        config=IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=2),
    )
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    # All running tasks should have different hosts
    def different_hosts_for_running_tasks():
        hosts = set()
        num_running, num_pending = 0, 0
        tasks = job.list_tasks().value
        for id, t in tasks.items():
            if t.runtime.state == task_pb2.TaskState.Value("RUNNING"):
                num_running = num_running + 1
                hosts.add(t.runtime.host)
            if t.runtime.state == task_pb2.TaskState.Value("PENDING"):
                num_pending = num_pending + 1

        # number of running tasks should be equal to the size of the hosts set
        # there should be 1 task in PENDING
        return len(hosts) == num_running and num_pending == 1

    job.wait_for_condition(different_hosts_for_running_tasks)

    job.stop()
    job.wait_for_state(goal_state="KILLED")


# Test placement of job without exclusive constraint and verify that the tasks
# don't run on exclusive host
def test_placement_non_exclusive_job(exclusive_host, peloton_client):
    # Set number of instances to be a few more than what can run on
    # 2 (non-exclusive) hosts
    job = Job(
        client=peloton_client,
        job_file="long_running_job.yaml",
        config=IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=2),
        options=[with_instance_count(12)],
    )
    job.job_config.defaultConfig.command.value = "sleep 10"
    job.create()
    job.wait_for_state()

    # check that none of them ran on exclusive host
    task_infos = job.list_tasks().value
    for instance_id, task_info in task_infos.items():
        assert "exclusive" not in task_info.runtime.host


# Test placement of job with exclusive constraint and verify that the tasks
# run only on exclusive host
def test_placement_exclusive_job(exclusive_host, peloton_client):
    excl_constraint = task_pb2.Constraint(
        type=1,  # Label constraint
        labelConstraint=task_pb2.LabelConstraint(
            kind=2,  # Host
            condition=2,  # Equal
            requirement=1,
            label=peloton_pb2.Label(
                key="peloton/exclusive", value="exclusive-test-label"
            ),
        ),
    )
    # Set number of instances to be a few more than what can run on
    # a single exclusive host
    job = Job(
        client=peloton_client,
        job_file="long_running_job.yaml",
        config=IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=2),
        options=[with_constraint(excl_constraint), with_instance_count(6)],
    )
    job.job_config.defaultConfig.command.value = "sleep 10"
    job.create()
    job.wait_for_state()

    # check that all of them ran on exclusive host
    task_infos = job.list_tasks().value
    for instance_id, task_info in task_infos.items():
        assert "exclusive" in task_info.runtime.host
