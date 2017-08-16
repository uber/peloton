import pytest

from job import Job


pytestmark = [pytest.mark.default, pytest.mark.task]


def test__upgrade_task_changes_command():
    job = Job(job_file='long_running_job.yaml')
    job.job_config.instanceCount = 1
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    config = job.get_config()
    config.defaultConfig.command.value = 'true'

    job.upgrade(config)

    job.wait_for_state(goal_state='SUCCEEDED')


@pytest.mark.pcluster
def test__start_upgrade_and_restart_jobmgr_will_changes_command(jobmgr):
    job = Job(job_file='long_running_job.yaml')
    job.job_config.instanceCount = 1
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    config = job.get_config()
    config.defaultConfig.command.value = 'true'

    job.upgrade(config)
    jobmgr.restart()

    job.wait_for_state(goal_state='SUCCEEDED')
