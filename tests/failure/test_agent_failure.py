import time
import random

from tests.integration.common import IntegrationTestConfig


class TestMesosAgentFailure(object):
    def test_agent_restart(self, failure_tester):
        """
        Restart Mesos agents after creating a job and verify that
        the job is restarted and succeeds. Use a non-default restart-policy
        so that lost tasks will be retried.
        """
        job = failure_tester.job(job_file="long_running_job.yaml")
        job.job_config.defaultConfig.command.value = "sleep 50"
        job.job_config.defaultConfig.restartPolicy.maxFailures = 10
        job.create()
        job.wait_for_state(goal_state="RUNNING")

        assert 0 != failure_tester.fw.restart(failure_tester.mesos_agent)

        job.wait_for_state()

    def test_lost_task_no_retry(self, failure_tester):
        """
        Restart Mesos agents after creating a job so that tasks get lost;
        verify that the job gets killed because there are no retries with
        default restart policy.
        """
        job = failure_tester.job(job_file="long_running_job.yaml")
        job.create()
        job.wait_for_state(goal_state="RUNNING")

        assert 0 != failure_tester.fw.restart(failure_tester.mesos_agent)

        job.wait_for_state(goal_state="FAILED")

    def test_in_place_update_with_agent_restart(self, failure_tester):
        """
        Restart Mesos agents after updating a stateless job and verify that
        the update finishes successfully
        """
        job = failure_tester.stateless_job()
        # increase max retry since it would take more time for in-place update
        # to get placed when agent restarts
        job.config = IntegrationTestConfig(max_retry_attempts=300)
        job.create()
        job.wait_for_all_pods_running()

        update = failure_tester.stateless_update(
            job=job,
            updated_job_file="test_update_stateless_job_spec.yaml",
        )
        update.create(in_place=True)

        time.sleep(random.randint(1, 10))

        assert 0 != failure_tester.fw.restart(failure_tester.mesos_agent)

        update.wait_for_state(goal_state="SUCCEEDED")

    def test_in_place_update_with_agent_stop(self, failure_tester):
        """
        Stop Mesos agents after updating a stateless job and verify that
        the update finishes successfully
        """
        job = failure_tester.stateless_job()
        # increase max retry since it would take more time for in-place update
        # to get placed when agent restarts
        job.config = IntegrationTestConfig(max_retry_attempts=300)
        job.create()
        job.wait_for_all_pods_running()

        update = failure_tester.stateless_update(
            job=job,
            updated_job_file="test_update_stateless_job_spec.yaml",
        )
        update.create(in_place=True)

        time.sleep(random.randint(1, 10))

        assert 0 != failure_tester.fw.stop(failure_tester.mesos_agent)

        update.wait_for_state(goal_state="SUCCEEDED")

        assert 0 != failure_tester.fw.start(failure_tester.mesos_agent)

    def test__kill_mesos_agent_makes_task_resume_stateless_job(self, failure_tester):
        '''
        Restart Mesos agents after creating a stateless job
        and verify that the job is still running
        '''
        stateless_job = failure_tester.stateless_job()
        stateless_job.create()
        stateless_job.wait_for_state(goal_state="RUNNING")

        assert 0 != failure_tester.fw.restart(failure_tester.mesos_agent)

        stateless_job.wait_for_state(goal_state="RUNNING")
