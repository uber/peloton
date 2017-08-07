import pytest

from job import Job


pytestmark = [pytest.mark.default, pytest.mark.task]


def test__stop_all_tasks_kills_tasks_and_job():
    job = Job(job_file='long_running_job.yaml')
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    job.stop()
    job.wait_for_state(goal_state='KILLED')


def test__stop_tasks_when_mesos_master_down_kills_tasks_when_started(
        mesos_master):
    job = Job(job_file='long_running_job.yaml')
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    mesos_master.stop()

    job.stop()

    mesos_master.start()

    job.wait_for_state(goal_state='KILLED')
