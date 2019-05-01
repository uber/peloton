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
