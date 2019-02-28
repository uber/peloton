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
        resp = query_jobs(respool_path)
        if len(resp.records) == 0:
            return
        time.sleep(2)

    assert False, 'timed out waiting for jobs to be deleted'
