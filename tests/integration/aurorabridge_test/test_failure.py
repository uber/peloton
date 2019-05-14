import pytest
import time
import random

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    get_job_update_request,
    wait_for_rolled_forward,
)

pytestmark = [pytest.mark.default,
              pytest.mark.aurorabridge]


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
