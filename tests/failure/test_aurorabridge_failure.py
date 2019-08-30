import logging
import pytest
import random
import time

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    assert_keys_equal,
    get_job_update_request,
    get_update_status,
    start_job_update,
    wait_for_rolled_forward,
    wait_for_update_status,
    wait_for_running,
)

log = logging.getLogger(__name__)


class TestAurorabridgeFailure(object):
    def test__simple_update_with_restart_component(self, failure_tester):
        """
        Start an update, and restart jobmgr, resmgr, hostmgr & mesos master.
        """
        res = failure_tester.aurorabridge_client.start_job_update(
            get_job_update_request('test_dc_labrat_large_job.yaml'),
            'start job update test/dc/labrat_large_job')

        # wait for sometime for jobmgr goal state engine to kick-in
        time.sleep(random.randint(1, 10))

        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()

        # wait for sometime to enqueue gangs
        time.sleep(random.randint(1, 10))

        # clear any admission and queues
        leader = failure_tester.fw.get_leader_info(failure_tester.resmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.resmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.resmgr, leader)
        failure_tester.reset_client()

        # wait for sometime to acquire host lock
        time.sleep(random.randint(1, 10))

        # clear host `placing` lock
        leader = failure_tester.fw.get_leader_info(failure_tester.hostmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.hostmgr, leader)
        failure_tester.reset_client()
        time.sleep(random.randint(1, 10))

        # restart mesos master to jumble up host manager state
        assert 0 != failure_tester.fw.restart(failure_tester.mesos_master)

        #  Sleep to help the cluster to stabilize
        time.sleep(10)

        wait_for_rolled_forward(
            failure_tester.aurorabridge_client, res.key, timeout_secs=200)
        res = failure_tester.aurorabridge_client.get_tasks_without_configs(api.TaskQuery(
            jobKeys={res.key.job},
            statuses={api.ScheduleStatus.RUNNING}
        ))
        assert len(res.tasks) == 10

    def test__simple_update_tasks_reconcile(self, failure_tester):
        """
        Restart host manager and mesos master multiple times,
        to make sure mesos tasks are reconciled correctly.
        """
        res = failure_tester.aurorabridge_client.start_job_update(
            get_job_update_request('test_dc_labrat_large_job.yaml'),
            'start job update test/dc/labrat_large_job')

        # wait for sometime for jobmgr goal state engine to kick-in
        time.sleep(random.randint(1, 10))

        # First restart
        leader = failure_tester.fw.get_leader_info(failure_tester.hostmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.hostmgr, leader)
        failure_tester.reset_client()

        time.sleep(random.randint(1, 5))

        assert 0 != failure_tester.fw.restart(failure_tester.mesos_master)

        # Second restart
        leader = failure_tester.fw.get_leader_info(failure_tester.hostmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.hostmgr, leader)
        failure_tester.reset_client()

        time.sleep(random.randint(1, 5))

        assert 0 != failure_tester.fw.restart(failure_tester.mesos_master)

        # Third restart
        leader = failure_tester.fw.get_leader_info(failure_tester.hostmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.hostmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.hostmgr, leader)
        failure_tester.reset_client()

        time.sleep(random.randint(1, 5))

        assert 0 != failure_tester.fw.restart(failure_tester.mesos_master)

        # Sleep to help the cluster to stabilize
        time.sleep(10)

        wait_for_rolled_forward(failure_tester.aurorabridge_client, res.key)
        res = failure_tester.aurorabridge_client.get_tasks_without_configs(api.TaskQuery(
            jobKeys={res.key.job},
            statuses={api.ScheduleStatus.RUNNING}
        ))
        assert len(res.tasks) == 10

    def test__get_job_update_details__restart_jobmgr(self, failure_tester):
        """
        Start an update, call getJobUpdateDetails, restart jobmgr:
        1. Before recovery finishes, expect error
        2. After recovery finishes, expect getJobUpdateDetails to include the
        correct job
        """
        # start job update paused
        req = get_job_update_request('test_dc_labrat_large_job.yaml')
        req.settings.updateGroupSize = 10
        req.settings.blockIfNoPulsesAfterMs = 604800000
        res = failure_tester.aurorabridge_client.start_job_update(
            req, 'start job update test/dc/labrat_large_job')

        job_update_key = res.key
        job_key = job_update_key.job
        wait_for_update_status(
            failure_tester.aurorabridge_client,
            job_update_key,
            {api.JobUpdateStatus.ROLLING_FORWARD},
            api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE,
            120,
        )

        def check():
            res = failure_tester.aurorabridge_client.get_job_update_details(
                None, api.JobUpdateQuery(jobKey=job_key))
            assert len(res.detailsList) == 1
            assert res.detailsList[0].update.summary.key.job == job_key

        def wait():
            start = time.time()
            while time.time() - start < 120:
                try:
                    check()
                    break
                except Exception as e:
                    log.info('getJobUpdateDetails failed: %s, retrying...', e)
                    time.sleep(0.5)
            else:
                assert False, \
                    'Timed out waiting for getJobUpdateDetails endpoint to recover'

        # verify getJobUpdateDetailsResult
        check()

        # restart jobmgr
        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()

        wait()

        # wait additional time before proceeding, so that jobmgr has leader elected
        log.info('Wating 5 seconds before proceeding')
        time.sleep(5)

        # resume update
        failure_tester.aurorabridge_client.pulse_job_update(job_update_key)
        wait_for_rolled_forward(
            failure_tester.aurorabridge_client, job_update_key)
        wait_for_running(failure_tester.aurorabridge_client, job_key)

        # verify getJobUpdateDetailsResult
        check()

        # restart jobmgr
        leader = failure_tester.fw.get_leader_info(failure_tester.jobmgr)
        assert leader
        assert 0 != failure_tester.fw.restart(failure_tester.jobmgr, "leader")
        failure_tester.wait_for_leader_change(failure_tester.jobmgr, leader)
        failure_tester.reset_client()

        wait()

        # wait additional time before exiting, so that jobmgr has leader elected
        log.info('Wating 5 seconds before exiting')
        time.sleep(5)
