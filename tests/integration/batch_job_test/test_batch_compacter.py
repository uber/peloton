import pytest
import time

from tests.integration.job import Job, kill_jobs, with_instance_count
from tests.integration.common import IntegrationTestConfig
from tests.integration.client import Client, with_private_stubs
from tests.integration.pool import Pool
from tests.integration.host import ensure_host_pool
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.peloton.private.hostmgr.hostsvc import hostsvc_pb2 as hostmgr

# Mark test module so that we can run tests by tags
pytestmark = [
    pytest.mark.default,
]

BATCH_RESERVED = "batch_reserved"
SHARED = "shared"
STATELESS = "stateless"


@pytest.mark.skip(reason="Till we enable host pool config for placement engine")
def test__dynamic_partition_pool_restrictions():
    ensure_host_pool(BATCH_RESERVED, 1)
    ensure_host_pool(SHARED, 1)
    ensure_host_pool(STATELESS, 1)

    # Job has two instances with 3 cpus each.
    # Only one instance will run.
    npjob = Job(
        job_file="test_non_preemptible_job.yaml",
        config=IntegrationTestConfig(max_retry_attempts=100),
    )
    npjob.create()
    npjob.wait_for_state(goal_state='RUNNING')

    count = 0
    for t in npjob.get_tasks():
        if npjob.get_task(t).state_str == "PENDING":
            count = count + 1

    assert count == 1

    # Stateless job has 4 instances with host limit 1
    # so only one instance will run
    sjob = Job(
        job_file="test_stateless_job_host_limit_1.yaml",
        config=IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=2),
    )
    sjob.create()
    sjob.wait_for_state(goal_state="RUNNING")

    count = 0
    for t in sjob.get_tasks():
        if sjob.get_task(t).state_str == "PENDING":
            count = count + 1

    assert count == 3

    # Preemptible batch job has 12 instances with 1 CPU each,
    # so 4 instances will run.
    pjob = Job(
        job_file="test_preemptible_job.yaml",
        config=IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=2),
    )
    pjob.create()
    pjob.wait_for_state(goal_state="RUNNING")

    count = 0
    for t in pjob.get_tasks():
        if pjob.get_task(t).state_str == "PENDING":
            count = count + 1

    assert count == 8

    # Stop all jobs
    npjob.stop()
    sjob.stop()
    pjob.stop()
