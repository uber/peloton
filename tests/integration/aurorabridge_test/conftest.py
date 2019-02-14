import pytest

from tests.integration.aurorabridge_test.client import Client
from tests.integration.aurorabridge_test.util import (
    check_response_ok,
)


@pytest.fixture
def client():
    client = Client()

    yield client

    # Teardown all jobs.
    res = client.get_jobs('')
    check_response_ok(res)
    for config in res.result.getJobsResult.configs:
        res = client.kill_tasks(config.key, range(config.instanceCount), 'teardown jobs')
        check_response_ok(res)
