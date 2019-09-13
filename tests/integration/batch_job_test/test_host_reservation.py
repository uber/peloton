import pytest


from tests.integration.job import Job, kill_jobs, with_instance_count
from tests.integration.common import IntegrationTestConfig
from tests.integration.client import with_private_stubs
from tests.integration.pool import Pool
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.peloton.private.hostmgr.hostsvc import hostsvc_pb2 as hostmgr

# Mark test module so that we can run tests by tags
pytestmark = [
    pytest.mark.skip(reason="host reservation feature is turned off"),
    pytest.mark.default,
    pytest.mark.reserve,
]


def respool(request, pool_file, peloton_client):
    pool = Pool(
        client=peloton_client,
        config=IntegrationTestConfig(pool_file=pool_file),
    )

    # teardown
    def delete_pool():
        pool.delete()

    request.addfinalizer(delete_pool)
    return pool


@pytest.fixture(scope="function", autouse=True)
def hostreservepool(request, peloton_client):
    return respool(request, pool_file='test_hostreservation_pool.yaml',
                   peloton_client=peloton_client)


# Test the basic host reservation scenario:
# 1. Create job 1 with 3 tasks which run on each of 3 hosts
# 2. Create job 2 with one task which doesn't have enough resource on any host
#    and the task changes to RESERVED state
# 3. Job 2 changes to RUNNING state and finishes
def test__tasks_reserve_execution(hostreservepool, peloton_client):
    p_job_median = Job(
        client=peloton_client,
        job_file='test_hostreservation_job_median.yaml',
        pool=hostreservepool,
        config=IntegrationTestConfig(
            max_retry_attempts=100,
            sleep_time_sec=1),
    )

    p_job_median.create()
    p_job_median.wait_for_state(goal_state='RUNNING')

    # we should have all 3 tasks in running state
    def all_running():
        return all(t.state == task.RUNNING for t in p_job_median.get_tasks().values())

    p_job_median.wait_for_condition(all_running)

    # decorate the client to add peloton private API stubs
    client = with_private_stubs(peloton_client)

    p_job_large = Job(
        job_file='test_hostreservation_job_large.yaml',
        pool=hostreservepool,
        config=IntegrationTestConfig(
            sleep_time_sec=1,
            max_retry_attempts=300),
        options=[with_instance_count(1)],
        client=client,
    )
    p_job_large.create()
    p_job_large.wait_for_state(goal_state='PENDING')

    request = hostmgr.GetHostsByQueryRequest()

    # task should get into reserved state and RUNNING state
    t1 = p_job_large.get_task(0)
    t1.wait_for_pending_state(goal_state="RESERVED")

    # the task is running on reserved host
    def get_reserved_host():
        resp = client.hostmgr_svc.GetHostsByQuery(
            request,
            metadata=p_job_large.client.hostmgr_metadata,
            timeout=p_job_large.config.rpc_timeout_sec,)

        for h in resp.hosts:
            if h.status == 'reserved':
                return h.hostname
        return ''

    def is_reserved():
        return get_reserved_host() != ''

    p_job_large.wait_for_condition(is_reserved)
    reserved_host = get_reserved_host()

    t1.wait_for_pending_state(goal_state="RUNNING")
    assert reserved_host == t1.get_info().runtime.host

    # p_job_large should succeed
    p_job_large.wait_for_state()

    # no host is in reserved state
    response = client.hostmgr_svc.GetHostsByQuery(
        request,
        metadata=p_job_large.client.hostmgr_metadata,
        timeout=p_job_large.config.rpc_timeout_sec,)
    for host in response.hosts:
        assert host.status != 'reserved'

    kill_jobs([p_job_median, p_job_large])
