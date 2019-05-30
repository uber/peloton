import logging
import pytest
import random
import time

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    get_job_update_request,
    wait_for_rolled_forward,
    wait_for_running,
    wait_for_update_status,
)

pytestmark = [pytest.mark.default,
              pytest.mark.aurorabridge]

log = logging.getLogger(__name__)


def test__simple_update_with_restart_component(
        client,
        jobmgr,
        resmgr,
        hostmgr,
        mesos_master):
    """
    Start an update, and restart jobmgr, resmgr, hostmgr & mesos master.
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')

    # wait for sometime for jobmgr goal state engine to kick-in
    time.sleep(random.randint(1, 10))
    jobmgr.restart()

    # wait for sometime to enqueue gangs
    time.sleep(random.randint(1, 10))

    # clear any admission and queues
    resmgr.restart()

    # wait for sometime to acquire host lock
    time.sleep(random.randint(1, 10))

    # clear host `placing` lock
    hostmgr.restart()
    time.sleep(random.randint(1, 10))

    # restart mesos master to jumble up host manager state
    mesos_master.restart()

    wait_for_rolled_forward(client, res.key)
    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={res.key.job},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 10


def test__simple_update_events_purge(
        client,
        jobmgr,
        resmgr):
    """
    Restart job manager and resource manager multiple times,
    to make sure mesos task status update events are purged
    correctly.
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')

    # wait for sometime for jobmgr goal state engine to kick-in
    time.sleep(random.randint(1, 10))

    # First restart
    jobmgr.restart()
    time.sleep(random.randint(1, 5))
    resmgr.restart()
    time.sleep(random.randint(1, 5))

    # Second restart
    jobmgr.restart()
    time.sleep(random.randint(1, 5))
    resmgr.restart()
    time.sleep(random.randint(1, 5))

    # Third restart
    jobmgr.restart()
    time.sleep(random.randint(1, 5))
    resmgr.restart()

    wait_for_rolled_forward(client, res.key)
    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={res.key.job},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 10


def test__simple_update_tasks_reconcile(
        client,
        hostmgr,
        mesos_master):
    """
    Restart host manager and mesos master multiple times,
    to make sure mesos tasks are reconciled correctly.
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')

    # wait for sometime for jobmgr goal state engine to kick-in
    time.sleep(random.randint(1, 10))

    # First restart
    hostmgr.restart()
    time.sleep(random.randint(1, 5))
    mesos_master.restart()

    # Second restart
    hostmgr.restart()
    time.sleep(random.randint(1, 5))
    mesos_master.restart()

    # Third restart
    hostmgr.restart()
    time.sleep(random.randint(1, 5))
    mesos_master.restart()

    wait_for_rolled_forward(client, res.key)
    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={res.key.job},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 10


def test__get_job_update_details__restart_jobmgr(client,
                                                 jobmgr,
                                                 resmgr,
                                                 hostmgr,
                                                 mesos_master):
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
    res = client.start_job_update(req,
                                  'start job update test/dc/labrat_large_job')

    job_update_key = res.key
    job_key = job_update_key.job
    wait_for_update_status(
        client,
        job_update_key,
        {api.JobUpdateStatus.ROLLING_FORWARD},
        api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE,
        120,
    )

    def check():
        res = client.get_job_update_details(None,
                                            api.JobUpdateQuery(jobKey=job_key))
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
    jobmgr.restart()
    wait()

    # wait additional time before proceeding, so that jobmgr has leader elected
    log.info('Wating 5 seconds before proceeding')
    time.sleep(5)

    # resume update
    client.pulse_job_update(job_update_key)
    wait_for_rolled_forward(client, job_update_key)
    wait_for_running(client, job_key)

    # verify getJobUpdateDetailsResult
    check()

    # restart jobmgr
    jobmgr.restart()
    wait()

    # wait additional time before exiting, so that jobmgr has leader elected
    log.info('Wating 5 seconds before exiting')
    time.sleep(5)
