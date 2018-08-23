class TestMesosAgentFailure(object):

    def test_agent_restart(self, failure_tester):
        """
        Restart Mesos agents after creating a job and verify that
        the job is restarted and succeeds.
        """
        job = failure_tester.job(job_file='long_running_job.yaml')
        job.job_config.defaultConfig.command.value = "sleep 30"
        job.create()
        job.wait_for_state("RUNNING")

        assert 0 != failure_tester.fw.restart(failure_tester.mesos_agent)

        job.wait_for_state()
