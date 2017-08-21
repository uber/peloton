import pytest

from job import IntegrationTestConfig, Job


# Mark test module so that we can run tests by tags
pytestmark = [pytest.mark.default, pytest.mark.job]


def test__create_batch_job():
    job = Job(job_file='test_job_no_container.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state()


def test__create_job():
    job = Job()
    job.create()
    job.wait_for_state()


def test__stop_long_running_batch_job_immediately(
        mesos_master, jobmgr):
    job = Job(job_file='long_running_job.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.job_config.instanceCount = 100
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    job.stop()
    job.wait_for_state(goal_state='KILLED')
