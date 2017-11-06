import pytest
from job import IntegrationTestConfig, Job


# Mark test module so that we can run tests by tags
pytestmark = [pytest.mark.default, pytest.mark.job]


@pytest.mark.smoketest
def test__create_batch_job():
    job = Job(job_file='test_job_no_container.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state()


@pytest.mark.smoketest
def test__create_job():
    job = Job()
    job.create()
    job.wait_for_state()


@pytest.mark.smoketest
def test__stop_long_running_batch_job_immediately(
        mesos_master, jobmgr):
    job = Job(job_file='long_running_job.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.job_config.instanceCount = 100
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    job.stop()
    job.wait_for_state(goal_state='KILLED')


def test__create_a_batch_job_and_restart_jobmgr_completes_jobs(jobmgr):
    job = Job(job_file='test_job_no_container.yaml',
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()

    # Restart immediately. That will lave some fraction unallocated and another
    # fraction initialized.
    jobmgr.restart()

    job.wait_for_state()