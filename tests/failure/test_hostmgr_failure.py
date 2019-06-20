from retry import retry

from peloton_client.pbgen.peloton.api.v0.host.svc import (
    host_svc_pb2 as host_svc,
)

from tests.integration.common import IntegrationTestConfig


class TestHostMgrFailure(object):
    def test_hostmgr_failover(self, failure_tester):
        """
        Test host-manager fails over to follower when leader is
        restarted.
        """
        hosts1 = self._get_hosts(failure_tester)

        leader1 = failure_tester.fw.get_leader_info(failure_tester.hostmgr)
        assert leader1
        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")

        failure_tester.wait_for_leader_change(failure_tester.hostmgr, leader1)
        failure_tester.reset_client()

        # verify that we can query the new leader
        def check_hosts():
            hosts2 = self._get_hosts(failure_tester)
            return len(hosts1) == len(hosts2)

        failure_tester.wait_for_condition(check_hosts)

    def test_hostmgr_restart_job_succeeds(self, failure_tester):
        """
        Restart host-manager leader after creating a job and verify that
        the job runs to completion.
        """
        job = failure_tester.job(job_file="test_job_no_container.yaml")
        job.create()

        # Restart immediately, so that tasks will be in various
        # stages of launch
        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")

        job.wait_for_state()

    def test__in_place_update_hostmgr_restart(self, failure_tester):
        """
        # Restart hostmgr leader
        # in-place update would not bloack due to hostmgr restarts
        """
        # need extra retry attempts, since in-place update would need more time
        # to process given hostmgr would be restarted
        job1 = failure_tester.stateless_job(
            job_file="test_stateless_job_spec.yaml",
            config=IntegrationTestConfig(max_retry_attempts=300),
        )
        job1.create()
        job1.wait_for_all_pods_running()

        update1 = failure_tester.update(
            job=job1,
            updated_job_file="test_update_stateless_job_spec.yaml",
        )
        update1.create(in_place=True)

        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")

        update1.wait_for_state(goal_state="SUCCEEDED")

    @retry(tries=10, delay=5)
    def _get_hosts(self, failure_tester):
        req = host_svc.QueryHostsRequest()

        resp = failure_tester.client.host_svc.QueryHosts(
            req,
            metadata=failure_tester.client.hostmgr_metadata,
            timeout=failure_tester.integ_config.rpc_timeout_sec,
        )
        return resp.host_infos
