import grpc
import time

import pytest

from tests.integration.aurorabridge_test.client import Client
from tests.integration.stateless_job import query_jobs, StatelessJob


@pytest.fixture
def client():
    client = Client()

    yield client

    # Delete all jobs
    _delete_jobs()


def _delete_jobs(respool_path='/AuroraBridge',
                 timeout_secs=240):
    resp = query_jobs(respool_path)

    for j in resp.records:
        job = StatelessJob(job_id=j.job_id.value)
        job.delete(force_delete=True)

    # wait for job deletion to complete
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            resp = query_jobs(respool_path)
            if len(resp.records) == 0:
                return
            time.sleep(2)
        except grpc.RpcError as e:
            # catch "not-found" error here because QueryJobs endpoint does
            # two db queries in sequence: "QueryJobs" and "GetUpdate".
            # However, when we delete a job, updates are deleted first,
            # there is a slight chance QueryJobs will fail to query the
            # update, returning "not-found" error.
            if e.code() == grpc.StatusCode.NOT_FOUND:
                time.sleep(2)
                continue
            raise

    assert False, 'timed out waiting for jobs to be deleted'
