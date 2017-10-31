import pytest
import time

from job import IntegrationTestConfig, Job


# Mark test module so that we can run tests by tags
pytestmark = [pytest.mark.default, pytest.mark.job]


@pytest.mark.pcluster
def test_too_large_gang_should_hang():
    job = Job(job_file='test_too_large_gang_job.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()

    for _ in range(10):
        job.wait_for_state(goal_state='PENDING', failed_state='FAILED')
        time.sleep(1)

    job.stop()


@pytest.mark.pcluster
def test_too_large_gang_should_hang_even_after_restart(jobmgr):
    job = Job(job_file='test_too_large_gang_job.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()

    for _ in range(10):
        job.wait_for_state(goal_state='PENDING', failed_state='FAILED')
        time.sleep(1)

    jobmgr.restart()

    for _ in range(10):
        job.wait_for_state(goal_state='PENDING', failed_state='FAILED')
        time.sleep(1)

    job.stop()
