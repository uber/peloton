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

    # Sleep to ensure lucene index converges
    time.sleep(10)

    wait_for_rolled_forward(client, res.key)
    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={res.key.job},
        statuses={api.ScheduleStatus.RUNNING}
    ))
    assert len(res.tasks) == 10
