import pytest

from job import IntegrationTestConfig, Job


# Mark test module so that we can run tests by tags
pytestmark = [pytest.mark.default, pytest.mark.job]


def test_create_batch_job():
    job = Job(job_file='test_job_no_container.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state()


def test_create_job():
    job = Job()
    job.create()
    job.wait_for_state()
