import pytest

from job import Job


pytestmark = [pytest.mark.default]


def test__unhealthy_task_is_killed():
    job = Job(job_file='unhealthy_job.yaml')
    job.create()

    job.wait_for_state(goal_state='KILLED')

    assert 4 == job.get_task(0).get_info().runtime.failureCount
