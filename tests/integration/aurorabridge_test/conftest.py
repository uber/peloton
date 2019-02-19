import pytest

from tests.integration.aurorabridge_test.client import Client
from tests.integration.stateless_job import query_jobs, StatelessJob


@pytest.fixture
def client():
    client = Client()

    yield client

    # Delete all jobs
    delete_jobs()


def delete_jobs(respool_path='/AuroraBridge'):
    resp = query_jobs(respool_path)

    for j in resp.records:
        job = StatelessJob(job_id=j.job_id.value)
        job.delete(force_delete=True)
