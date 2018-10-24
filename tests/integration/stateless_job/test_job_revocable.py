import pytest
import time

from tests.integration.job import Job
from peloton_client.pbgen.peloton.api.v0.task import task_pb2

pytestmark = [pytest.mark.default, pytest.mark.stateless]


# Resouce Pool reservation memory: 1000MB
# Slack Limit: 20% (default), 200MB
# Revocable Job Memory request: 100MB * 3(instances) = 300MB
def test__revocable_job_slack_limit():
    revocable_job = Job(job_file='test_stateless_job_revocable_slack_limit.yaml')
    revocable_job.create()
    revocable_job.wait_for_state(goal_state='RUNNING')

    # 2 tasks are running out of 3
    def partitial_tasks_running():
        count = 0
        for instance_id, task_info in revocable_job.list_tasks().value.items():
            if task_info.runtime.state == task_pb2.RUNNING:
                count += 1
        return count == 2

    revocable_job.wait_for_condition(partitial_tasks_running)

    # cleanup job from jobmgr
    revocable_job.stop()
    revocable_job.wait_for_state(goal_state='KILLED')


# Entitlement: MEMORY: 2000 (1000 reservation and 1000 limit)
# Launch non_revoacble & non_preemptible job with mem: 1000MB
# Launch non_revoacble & preemptible job with mem: 1000MB
# Launch revocable job -> no memory is left to satisfy request
# Kill any non_revocable job, and it will satisfy revocable job
def test__stop_nonrevocable_job_to_free_resources_for_revocable_job():
    non_revocable_job1 = Job(job_file='test_stateless_job_memory_large.yaml')
    non_revocable_job1.create()
    non_revocable_job2 = Job(job_file='test_stateless_preemptible_job_memory_large.yaml')
    non_revocable_job2.create()

    non_revocable_job1.wait_for_all_tasks_running()
    non_revocable_job2.wait_for_all_tasks_running()

    revocable_job = Job(job_file='test_stateless_job_revocable.yaml')
    revocable_job.create()

    # no tasks should be running
    def partitial_tasks_running():
        count = 0
        for instance_id, task_info in revocable_job.list_tasks().value.items():
            if task_info.runtime.state == task_pb2.RUNNING:
                count += 1
        return count == 0

    # give job 5 seconds to run, even after that no tasks should be running
    time.sleep(5)
    revocable_job.wait_for_condition(partitial_tasks_running)

    # stop non_revocable job to free up resources for revocable job
    non_revocable_job2.stop()
    non_revocable_job2.wait_for_state(goal_state='KILLED')

    # After non_revocable job is killed, all revocable tasks should be running
    revocable_job.wait_for_all_tasks_running()

    # cleanup jobs from jobmgr
    non_revocable_job1.stop()
    revocable_job.stop()
    non_revocable_job1.wait_for_state(goal_state='KILLED')
    revocable_job.wait_for_state(goal_state='KILLED')


# Simple scenario, where revocable and non-revocable job
# run simultaneously
# Physical cluster capacity cpus: 12
# Non-Revocable Job cpus: 5 (constrained by resource pool reservation)
# Revocable Job cpus: 5 * 2 = 10
def test__create_revocable_job():
    revocable_job1 = Job(job_file='test_stateless_job_revocable.yaml')
    revocable_job1.create()

    revocable_job2 = Job(job_file='test_stateless_job_revocable.yaml')
    revocable_job2.create()

    non_revocable_job = Job(job_file='test_stateless_job_cpus_large.yaml')
    non_revocable_job.create()

    revocable_job1.wait_for_all_tasks_running()
    revocable_job2.wait_for_all_tasks_running()
    non_revocable_job.wait_for_all_tasks_running()

    # cleanup jobs from jobmgr
    revocable_job1.stop()
    revocable_job2.stop()
    non_revocable_job.stop()
    revocable_job1.wait_for_state(goal_state='KILLED')
    revocable_job2.wait_for_state(goal_state='KILLED')
    non_revocable_job.wait_for_state(goal_state='KILLED')


# Revocable tasks are moved to revocable queue, so they do not block
# Non-Revocable tasks.
# Launch 2 revocable jobs, one will starve due to memory
def test__revocable_tasks_move_to_revocable_queue():
    revocable_job1 = Job(job_file='test_stateless_job_revocable.yaml')
    revocable_job1.create()
    revocable_job1.wait_for_all_tasks_running()

    # 1 task is running out of 3
    def partitial_tasks_running():
        count = 0
        for instance_id, task_info in revocable_job2.list_tasks().value.items():
            if task_info.runtime.state == task_pb2.RUNNING:
                count += 1
        return count == 1

    revocable_job2 = Job(job_file='test_stateless_job_revocable_slack_limit.yaml')
    revocable_job2.create()

    # sleep for 5 seconds to make sure job has enough time
    time.sleep(5)
    revocable_job2.wait_for_condition(partitial_tasks_running)

    non_revocable_job = Job(job_file='test_stateless_job.yaml')
    non_revocable_job.create()
    non_revocable_job.wait_for_all_tasks_running()

    # cleanup jobs from jobmgr
    revocable_job1.stop()
    revocable_job2.stop()
    non_revocable_job.stop()
    revocable_job1.wait_for_state(goal_state='KILLED')
    revocable_job2.wait_for_state(goal_state='KILLED')
    non_revocable_job.wait_for_state(goal_state='KILLED')
