import pytest
import time

from job import Job
from peloton_client.pbgen.peloton.api.job import job_pb2


pytestmark = [pytest.mark.default, pytest.mark.task]


def test__stop_start_all_tasks_kills_tasks_and_job():
    job = Job(job_file='long_running_job.yaml')
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    job.stop()
    job.wait_for_state(goal_state='KILLED')

    job.start()
    job.wait_for_state(goal_state='RUNNING')
    job.stop()


@pytest.mark.pcluster
def test__stop_start_tasks_when_mesos_master_down_kills_tasks_when_started(
        mesos_master):
    job = Job(job_file='long_running_job.yaml')
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    mesos_master.stop()
    job.stop()
    mesos_master.start()
    job.wait_for_state(goal_state='KILLED')

    mesos_master.stop()
    job.start()
    mesos_master.start()
    job.wait_for_state(goal_state='RUNNING')
    job.stop()


@pytest.mark.pcluster
def test__stop_start_tasks_when_mesos_master_down_and_jobmgr_restarts(
        mesos_master, jobmgr):
    job = Job(job_file='long_running_job.yaml')
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    mesos_master.stop()
    job.stop()
    jobmgr.restart()
    mesos_master.start()
    job.wait_for_state(goal_state='KILLED')

    mesos_master.stop()
    job.start()
    jobmgr.restart()
    mesos_master.start()
    job.wait_for_state(goal_state='RUNNING')
    job.stop()


@pytest.mark.pcluster
def test__kill_mesos_agent_makes_task_resume(mesos_agent):
    job = Job(job_file='long_running_job.yaml')
    job.job_config.type = job_pb2.SERVICE

    job.create()
    job.wait_for_state(goal_state='RUNNING')

    mesos_agent.restart()

    time.sleep(1.0)

    job.wait_for_state(goal_state='RUNNING')
    job.stop()


def test__task_restart_stops_and_starts_tasks_with_new_mesos_ids():
    job = Job(job_file='long_running_job.yaml')
    job.job_config.type = job_pb2.SERVICE

    job.create()
    job.wait_for_state(goal_state='RUNNING')
    tasks = job.get_tasks().values()

    # Save original mesos ids
    mesos_ids = {t.instance_id: t.mesos_task_id for t in tasks}
    job.restart()

    # Wait until mesos ids change for all the tasks
    def all_mesos_ids_are_updated():
        return all(mesos_ids[t.instance_id] != t.mesos_task_id for t in tasks)

    # Tasks shall be stopped before mesos id is changed
    job.wait_for_condition(all_mesos_ids_are_updated)

    # Make sure that tasks are running again
    job.wait_for_state(goal_state='RUNNING')
    job.stop()
