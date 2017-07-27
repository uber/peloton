import pytest

from test_job import create_job, IntegrationTestConfig

pytestmark = pytest.mark.load


def test_large_job():
    """
    Load test against a cluster, not local pcluster friendly
    """
    config = IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=10,
                                   job_file='test_job_no_container.yaml')
    config.job_config.instanceCount = 10000
    create_job(config)
