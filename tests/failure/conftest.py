import time
import pytest
import grpc
import failure_fixture
from retry import retry

from tests.integration.pool import deallocate_pools
from tests.integration.stateless_job import (
    list_jobs,
    StatelessJob
)


@pytest.fixture(scope="function")
def failure_tester(request):
    """
    Creates the test fixture for failure tests.
    """
    fix = failure_fixture.FailureFixture()
    print  # to format log output
    fix.setup()
    yield fix

    fix.reset_client()

    if is_aurora_bridge_test(request):
        # Delete all jobs without deallocating the pools
        cleanup_jobs(fix.client)
    else:
        # Stop all jobs and deallocate all the pools
        cleanup_test(fix.client)

    fix.teardown()


@pytest.fixture(scope="module", autouse=True)
def deallocate_pools_for_aurorabridge_tests(request):
    '''
    Deallocates pools when all aurorabridge tests complete
    '''
    yield

    if is_aurora_bridge_test(request):
        fix = failure_fixture.FailureFixture()
        fix.setup()
        deallocate_pools(fix.client)


def is_aurora_bridge_test(request):
    """
    Helper function to check if it is an aurorabridge test module
    """
    module_name = request.module.__name__
    return "aurorabridge" in module_name


def wait_for_deletion(client, timeout_secs):
    """
    Wait for job deletion to complete.
    """
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            jobs = [StatelessJob(job_id=s.job_id.value, client=client)
                    for s in list_jobs()]
            if len(jobs) == 0:
                return
            time.sleep(2)
        except grpc.RpcError as e:
            # Catch "not-found" error here because QueryJobs endpoint does
            # two db queries in sequence: "QueryJobs" and "GetUpdate".
            # However, when we delete a job, updates are deleted first,
            # there is a slight chance QueryJobs will fail to query the
            # update, returning "not-found" error.
            if e.code() == grpc.StatusCode.NOT_FOUND:
                time.sleep(2)
                continue
            raise

    assert False, "timed out waiting for jobs to be deleted"


def cleanup_jobs(client, timeout_secs=20):
    """
    Calls peloton API to delete all currently running jobs
    """
    jobs = [StatelessJob(job_id=s.job_id.value, client=client)
            for s in list_jobs()]

    for job in jobs:
        job.delete(force_delete=True)

    wait_for_deletion(client, timeout_secs)


def stop_jobs(client):
    '''
    Calls peloton API to terminate all batch jobs and stateless jobs
    '''
    # obtain a list of jobs from all resource pools and terminate them
    jobs = list_jobs()
    for job in jobs:
        job = StatelessJob(client=client, job_id=job.job_id.value)
        job.config.max_retry_attempts = 100
        job.stop()
        job.wait_for_terminated()


@retry(tries=10, delay=10)
def cleanup_test(client):
    '''
    Retry with delay if clean up fails.
    Clean up can fail since job index table can be slow to update.
    '''
    stop_jobs(client)
    deallocate_pools(client)
