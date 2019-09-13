import pytest

from tests.integration.job import Job
from tests.integration.common import IntegrationTestConfig


pytestmark = [pytest.mark.load, pytest.mark.random_order(disabled=True)]


def test_large_job(peloton_client):
    """
    Load test against a cluster, not local minicluster friendly
    """
    job = Job(
        client=peloton_client,
        job_file="test_job_no_container.yaml",
        config=IntegrationTestConfig(max_retry_attempts=1000),
    )
    job.job_config.instanceCount = 10000

    job.create()
    job.wait_for_state()
