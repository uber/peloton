import pytest
import time

from tests.integration.stateless_job import StatelessJob
from tests.integration.job import Job
from tests.integration.common import IntegrationTestConfig
from peloton_client.pbgen.peloton.api.v1alpha.pod import pod_pb2 as pod

pytestmark = [
    pytest.mark.default,
    pytest.mark.stateless,
    pytest.mark.revocable,
]


# 1) Resource Pool Memory: 1000 (reservation), 2000 (limit)
# Launch Non-Preemptible +  Non-Revocable Job:
#       Memory Request 1000, which will complete the reservation
# 2) Revocable Job: Memory request 50 < 200 slack limit
# 3) Launch Pre-emptible + Non-Revocable job:
#       Memory Request 1000, which will be satisfied using elastic resources
# Since cluster capacity is 2000, it will preempt revocable job for itself.
def test__preempt_revocable_job_to_run_non_revocable_job():
    non_revocable_job1 = StatelessJob(
        job_file="test_stateless_preemptible_job_memory_large_spec.yaml"
    )
    non_revocable_job1.create()
    non_revocable_job1.wait_for_state(goal_state="RUNNING")
    non_revocable_job1.wait_for_all_pods_running()

    revocable_job = StatelessJob(
        job_file="test_stateless_job_revocable_spec.yaml"
    )
    revocable_job.create()
    revocable_job.wait_for_state(goal_state="RUNNING")
    revocable_job.wait_for_all_pods_running()

    # launch second non-revocable job which will pre-empt revocable job
    non_revocable_job2 = StatelessJob(
        job_file="test_stateless_job_memory_large_spec.yaml"
    )
    non_revocable_job2.create()
    non_revocable_job2.wait_for_state(goal_state="RUNNING")
    non_revocable_job2.wait_for_all_pods_running()

    # no revocable job tasks should be running
    def zero_tasks_running():
        count = 0
        for pod_id in range(0, revocable_job.job_spec.instance_count):
            pod_state = revocable_job.get_pod(pod_id).get_pod_status().state
            if pod_state == pod.POD_STATE_RUNNING:
                count += 1
        return count == 0

    revocable_job.wait_for_condition(zero_tasks_running)

    revocable_job.stop()
    non_revocable_job1.stop()
    non_revocable_job2.stop()
    revocable_job.wait_for_terminated()
    non_revocable_job1.wait_for_terminated()
    non_revocable_job2.wait_for_terminated()


# Resouce Pool reservation memory: 1000MB
# Slack Limit: 20% (default), 200MB
# Revocable Job Memory request: 100MB * 3(instances) = 300MB
def test__revocable_job_slack_limit():
    revocable_job = StatelessJob(
        job_file="test_stateless_job_revocable_slack_limit_spec.yaml"
    )
    revocable_job.create()
    revocable_job.wait_for_state(goal_state="RUNNING")

    # 2 tasks are running out of 3
    def partial_tasks_running():
        count = 0
        for pod_id in range(0, revocable_job.job_spec.instance_count):
            pod_state = revocable_job.get_pod(pod_id).get_pod_status().state
            if pod_state == pod.POD_STATE_RUNNING:
                count += 1
        return count == 2

    revocable_job.wait_for_condition(partial_tasks_running)

    # cleanup job from jobmgr
    revocable_job.stop()
    revocable_job.wait_for_terminated()


# Entitlement: MEMORY: 2000 (1000 reservation and 1000 limit)
# Launch non_revoacble & non_preemptible job with mem: 1000MB
# Launch non_revoacble & preemptible job with mem: 1000MB
# Launch revocable job -> no memory is left to satisfy request
# Kill any non_revocable job, and it will satisfy revocable job
def test__stop_nonrevocable_job_to_free_resources_for_revocable_job():
    non_revocable_job1 = StatelessJob(
        job_file="test_stateless_job_memory_large_spec.yaml"
    )
    non_revocable_job1.create()
    non_revocable_job1.wait_for_state("RUNNING")

    non_revocable_job2 = StatelessJob(
        job_file="test_stateless_preemptible_job_memory_large_spec.yaml"
    )
    non_revocable_job2.create()
    non_revocable_job2.wait_for_state("RUNNING")

    non_revocable_job1.wait_for_all_pods_running()
    non_revocable_job2.wait_for_all_pods_running()

    revocable_job = StatelessJob(
        job_file="test_stateless_job_revocable_spec.yaml"
    )
    revocable_job.create()

    # no tasks should be running
    def no_task_running():
        count = 0
        for pod_id in range(0, revocable_job.job_spec.instance_count):
            pod_state = revocable_job.get_pod(pod_id).get_pod_status().state
            if pod_state == pod.POD_STATE_RUNNING:
                count += 1
        return count == 0

    # give job 5 seconds to run, even after that no tasks should be running
    time.sleep(5)
    revocable_job.wait_for_condition(no_task_running)

    # stop non_revocable job to free up resources for revocable job
    non_revocable_job2.stop()
    non_revocable_job2.wait_for_terminated()

    # After non_revocable job is killed, all revocable tasks should be running
    revocable_job.wait_for_all_pods_running()

    # cleanup jobs from jobmgr
    non_revocable_job1.stop()
    revocable_job.stop()
    non_revocable_job1.wait_for_terminated()
    revocable_job.wait_for_terminated()


# Simple scenario, where revocable and non-revocable job
# run simultaneously
# Physical cluster capacity cpus: 12
# Non-Revocable Job cpus: 5 (constrained by resource pool reservation)
# Revocable Job cpus: 5 * 2 = 10
def test__create_revocable_job():
    revocable_job1 = StatelessJob(
        job_file="test_stateless_job_revocable_spec.yaml"
    )
    revocable_job1.create()
    revocable_job1.wait_for_state(goal_state="RUNNING")
    revocable_job1.wait_for_all_pods_running()

    revocable_job2 = StatelessJob(
        job_file="test_stateless_job_revocable_spec.yaml"
    )
    revocable_job2.create()
    revocable_job2.wait_for_state(goal_state="RUNNING")
    revocable_job2.wait_for_all_pods_running()

    non_revocable_job = StatelessJob(
        job_file="test_stateless_job_cpus_large_spec.yaml"
    )
    non_revocable_job.create()
    non_revocable_job.wait_for_state(goal_state="RUNNING")
    non_revocable_job.wait_for_all_pods_running()

    # cleanup jobs from jobmgr
    revocable_job1.stop()
    revocable_job2.stop()
    non_revocable_job.stop()
    revocable_job1.wait_for_terminated()
    revocable_job2.wait_for_terminated()
    non_revocable_job.wait_for_terminated()


# Revocable tasks are moved to revocable queue, so they do not block
# Non-Revocable tasks.
# Launch 2 revocable jobs, one will starve due to memory
def test__revocable_tasks_move_to_revocable_queue():
    revocable_job1 = StatelessJob(
        job_file="test_stateless_job_revocable_spec.yaml"
    )
    revocable_job1.create()
    revocable_job1.wait_for_state(goal_state="RUNNING")
    revocable_job1.wait_for_all_pods_running()

    # 1 task is running out of 3
    def partial_tasks_running():
        count = 0
        for pod_id in range(0, revocable_job2.job_spec.instance_count):
            pod_state = revocable_job2.get_pod(pod_id).get_pod_status().state
            if pod_state == pod.POD_STATE_RUNNING:
                count += 1
        return count == 1

    revocable_job2 = StatelessJob(
        job_file="test_stateless_job_revocable_slack_limit_spec.yaml"
    )
    revocable_job2.create()

    # sleep for 5 seconds to make sure job has enough time
    time.sleep(5)
    revocable_job2.wait_for_condition(partial_tasks_running)

    non_revocable_job = StatelessJob(job_file="test_stateless_job_spec.yaml")
    non_revocable_job.create()
    non_revocable_job.wait_for_state("RUNNING")
    non_revocable_job.wait_for_all_pods_running()

    # cleanup jobs from jobmgr
    revocable_job1.stop()
    revocable_job2.stop()
    non_revocable_job.stop()
    revocable_job1.wait_for_terminated()
    revocable_job2.wait_for_terminated()
    non_revocable_job.wait_for_terminated()


# Launch Revocable batch & stateless job.
# Launch Non-Revocable batch and stateless job.
# Ensure batch jobs SUCCEED and stateless jobs RUNNING.
def test__simple_revocable_batch_and_stateless_colocate():
    revocable_stateless_job = StatelessJob(
        job_file="test_stateless_job_revocable_spec.yaml"
    )
    revocable_stateless_job.create()
    revocable_stateless_job.wait_for_state(goal_state="RUNNING")
    revocable_stateless_job.wait_for_all_pods_running()

    non_revocable_stateless_job = StatelessJob(
        job_file="test_stateless_job_spec.yaml"
    )
    non_revocable_stateless_job.create()
    non_revocable_stateless_job.wait_for_state(goal_state="RUNNING")
    non_revocable_stateless_job.wait_for_all_pods_running()

    revocable_batch_job = Job(
        job_file="test_job_revocable.yaml",
        config=IntegrationTestConfig(pool_file='test_stateless_respool.yaml'),
    )
    revocable_batch_job.create()
    revocable_batch_job.wait_for_state(goal_state="RUNNING")

    non_revocable_batch_job = Job(
        job_file="test_job.yaml",
        config=IntegrationTestConfig(pool_file='test_stateless_respool.yaml'),
    )
    non_revocable_batch_job.create()
    non_revocable_batch_job.wait_for_state(goal_state="RUNNING")

    revocable_batch_job.wait_for_state()
    non_revocable_batch_job.wait_for_state()
