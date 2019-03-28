import pytest
import time
import grpc

from tests.integration.aurorabridge_test.client import Client
from tests.integration.stateless_job import list_jobs, StatelessJob
from tests.integration.conftest import (
    setup_minicluster,
    teardown_minicluster,
)


@pytest.fixture(scope="session", autouse=True)
def bootstrap_cluster(request):
    tests_failed_before_module = request.session.testsfailed
    setup_minicluster()

    yield

    dump_logs = False
    if (request.session.testsfailed - tests_failed_before_module) > 0:
        dump_logs = True

    teardown_minicluster(dump_logs)


@pytest.fixture(scope="module", autouse=True)
def setup_cluster(request):
    """
    override parent module fixture
    """
    pass


@pytest.fixture
def client():
    client = Client()

    yield client

    # add 10 second sleep to let lucene index build
    # remove this hack, once we migrate to ListJobs
    time.sleep(10)

    # Delete all jobs
    _delete_jobs()


def _delete_jobs(timeout_secs=20):
    jobs = _list_jobs()

    for job in jobs:
        job.delete(force_delete=True)

    # Wait for job deletion to complete.
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            jobs = _list_jobs()
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

    assert False, 'timed out waiting for jobs to be deleted'


def _list_jobs():
    return [StatelessJob(job_id=s.job_id.value) for s in list_jobs()]
