import copy
from retry.api import retry_call

from tests.integration import pool as tpool


class TestResMgrFailure(object):
    def test_resmgr_failover(self, failure_tester):
        """
        Test res-manager fails over to follower when leader is
        restarted.
        """
        leader1 = failure_tester.fw.get_leader_info(failure_tester.resmgr)
        assert leader1
        assert 0 != failure_tester.fw.restart(failure_tester.resmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.resmgr, leader1)
        failure_tester.reset_client()

        # verify that we can do operations on new leader
        rp1 = self._get_test_pool(failure_tester, name="test_resmgr_failover")
        retry_call(
            rp1.ensure_exists, tries=10, delay=5, logger=failure_tester.log
        )
        rp1.delete()

    def test_resmgr_restart_job_succeeds(self, failure_tester):
        """
        Restart res-manager leader after creating a job and verify that
        the job runs to completion.
        """
        job = failure_tester.job(job_file="test_job_no_container.yaml")
        job.job_config.instanceCount = 8
        job.create()

        # Restart res-manager before all tasks complete
        assert 0 != failure_tester.fw.restart(failure_tester.resmgr, "leader")

        job.wait_for_state()

    def _get_test_pool(self, failure_tester, name="test-pool"):
        cfg = copy.deepcopy(failure_tester.integ_config)
        rp1_config = cfg.respool_config
        rp1_config.name = name
        for res in rp1_config.resources:
            res.reservation = 0.0
            res.limit = 0.0
        return tpool.Pool(cfg, failure_tester.client)
