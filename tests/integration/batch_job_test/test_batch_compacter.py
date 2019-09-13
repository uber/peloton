import pytest

from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.peloton.private.hostmgr.hostsvc import (
    hostsvc_pb2 as hostmgr)

from tests.integration.job import Job, kill_jobs, with_instance_count
from tests.integration.common import IntegrationTestConfig
import tests.integration.conf_util as util
from tests.integration.client import Client, with_private_stubs
from tests.integration.pool import Pool
from tests.integration.host import (
    ensure_host_pool,
    list_host_pools,
    delete_host_pool,
)


# Mark test module so that we can run tests by tags
pytestmark = [
    pytest.mark.hostpool,
]


def test__dynamic_partition_pool_restrictions(peloton_client):
    # we start with shared=1, batch_reserved=2
    # delete batch_reserved so that its hosts go to "default"
    delete_host_pool(util.HOSTPOOL_BATCH_RESERVED)

    # setup 3 host-pools with 1 host each
    ensure_host_pool(util.HOSTPOOL_BATCH_RESERVED, 1)
    ensure_host_pool(util.HOSTPOOL_SHARED, 1)
    ensure_host_pool(util.HOSTPOOL_STATELESS, 1)

    hostToPool = dict()
    resp = list_host_pools()
    for pool in resp.pools:
        for h in pool.hosts:
            hostToPool[h] = pool.name

    # Job has two instances with 3 cpus each.
    # Only one instance will run.
    npjob = Job(
        client=peloton_client,
        job_file="test_non_preemptible_job.yaml",
        config=IntegrationTestConfig(max_retry_attempts=100),
    )
    npjob.create()
    npjob.wait_for_state(goal_state='RUNNING')

    count = 0
    for t in npjob.get_tasks():
        if npjob.get_task(t).state_str == "PENDING":
            count = count + 1
        else:
            hostname = npjob.get_task(t).get_runtime().host
            assert hostToPool[hostname] == util.HOSTPOOL_BATCH_RESERVED

    assert count == 1

    # Stateless job has 4 instances with host limit 1
    # so only one instance will run
    sjob = Job(
        client=peloton_client,
        job_file="test_stateless_job_host_limit_1.yaml",
        config=IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=2),
    )
    sjob.create()
    sjob.wait_for_state(goal_state="RUNNING")

    count = 0
    for t in sjob.get_tasks():
        if sjob.get_task(t).state_str == "PENDING":
            count = count + 1
        else:
            hostname = sjob.get_task(t).get_runtime().host
            assert hostToPool[hostname] == util.HOSTPOOL_STATELESS

    assert count == 3

    # Preemptible batch job has 12 instances with 1 CPU each,
    # so 4 instances will run.
    pjob = Job(
        client=peloton_client,
        job_file="test_preemptible_job.yaml",
        config=IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=2),
    )
    pjob.create()
    pjob.wait_for_state(goal_state="RUNNING")

    count = 0
    for t in pjob.get_tasks():
        if pjob.get_task(t).state_str == "PENDING":
            count = count + 1
        else:
            hostname = pjob.get_task(t).get_runtime().host
            assert hostToPool[hostname] == util.HOSTPOOL_SHARED

    assert count == 8

    # Stop all jobs
    npjob.stop()
    sjob.stop()
    pjob.stop()
