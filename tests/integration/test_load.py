import pytest

from job import Job
from common import IntegrationTestConfig


pytestmark = pytest.mark.load


def test_large_job():
    """
    Load test against a cluster, not local pcluster friendly
    """
    job = Job(job_file='test_job_no_container.yaml',
              config=IntegrationTestConfig(max_retry_attempts=1000))
    job.job_config.instanceCount = 10000

    job.create()
    job.wait_for_state()
