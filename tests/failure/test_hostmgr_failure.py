import pytest
import random
import time
from retry import retry

from peloton_client.pbgen.peloton.api.v0.host.svc import (
    host_svc_pb2 as host_svc,
)

from tests.integration.conftest import in_place
from tests.integration.common import IntegrationTestConfig
from tests.integration.stateless_job_test.test_allocation import verify_allocation
from tests.integration.stateless_update import StatelessUpdate


class TestHostMgrFailure(object):
    @retry(tries=10, delay=5)
    def _get_hosts(self, failure_tester):
        '''
        helper function for test_hostmgr_failover
        '''
        req = host_svc.QueryHostsRequest()

        resp = failure_tester.client.host_svc.QueryHosts(
            req,
            metadata=failure_tester.client.hostmgr_metadata,
            timeout=failure_tester.integ_config.rpc_timeout_sec,
        )
        return resp.host_infos

    def test_hostmgr_failover(self, failure_tester):
        """
        Test host-manager fails over to follower when leader is
        restarted.
        """
        hosts1 = self._get_hosts(failure_tester)

        leader = failure_tester.fw.get_leader_info(failure_tester.hostmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.hostmgr, leader)
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
        leader = failure_tester.fw.get_leader_info(failure_tester.hostmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.hostmgr, leader)
        failure_tester.reset_client()
        job.client = failure_tester.client

        job.wait_for_state()

    def test__in_place_update_hostmgr_restart(self, failure_tester, in_place):
        """
        Restart hostmgr leader in-place update would not bloack due
        to hostmgr restarts
        """
        # need extra retry attempts, since in-place update would need more time
        # to process given hostmgr would be restarted
        job1 = failure_tester.stateless_job(
            job_file="test_stateless_job_spec.yaml",
            config=IntegrationTestConfig(max_retry_attempts=300),
        )
        job1.create()
        job1.wait_for_all_pods_running()

        update1 = failure_tester.stateless_update(
            job=job1,
            updated_job_file="test_update_stateless_job_spec.yaml",
            config=job1.config,
        )
        update1.create(in_place=True)

        leader = failure_tester.fw.get_leader_info(failure_tester.hostmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.hostmgr, leader)
        failure_tester.reset_client()
        job1.client = failure_tester.client

        update1.wait_for_state(goal_state="SUCCEEDED")
