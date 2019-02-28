class TestJobMgrFailure(object):

    def test_jobmgr_failover(self, failure_tester):
        """
        Test job-manager fails over to follower when leader is
        restarted.
        """
        leader1 = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader1
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")

        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader1)

        # verify that we can create jobs after failover
        job = failure_tester.job(job_file='test_job_no_container.yaml')
        job.job_config.instanceCount = 10
        job.create()

        job.wait_for_state()

    def test_jobmgr_restart_job_succeeds(self, failure_tester):
        """
        Restart job-manager leader after creating a job and verify that
        the job runs to completion.
        """
        job = failure_tester.job(job_file='test_job_no_container.yaml')
        job.create()

        # Restart immediately, so that tasks will be in various
        # stages of launch
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")

        job.wait_for_state()
