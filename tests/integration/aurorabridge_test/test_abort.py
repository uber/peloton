import pytest
import time
from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    get_job_update_request,
    wait_for_update_status,
    get_update_status,
    wait_for_rolled_forward,
)

pytestmark = [pytest.mark.default,
              pytest.mark.aurorabridge,
              pytest.mark.random_order(disabled=True)]

# TODO (varung):
# - Create an update, let it auto rollback and abort it.
# - Create an update, do manual rollback and abort it.
# - Create an update, do manual rollback with pulsed and abort it.
# - Create an update, do manual rollback, pause it and then abort it.
# - Abort an update, after 1st instance is RUNNING.


def test__deploy_on_aborted_update(client):
    """
    Deploy an update, and abort half-way. Then re-deploy
    same update. Updated instances should not restart again.
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job.yaml'),
        'start job update test/dc/labrat_large_job')

    # Few instances start
    time.sleep(5)

    client.abort_job_update(res.key, 'abort update')
    wait_for_update_status(
        client,
        res.key,
        {api.JobUpdateStatus.ROLLING_FORWARD},
        api.JobUpdateStatus.ABORTED)

    # Not all instances were created
    res = client.get_tasks_without_configs(api.TaskQuery(jobKeys={res.key.job}))
    assert len(res.tasks) < 10

    # deploy same update, should impact remaining instances
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_large_job_diff_executor.yaml'),
        'start job update test/dc/labrat_large_job_diff_executor')
    wait_for_rolled_forward(client, res.key)

    # All instances are created
    res = client.get_tasks_without_configs(api.TaskQuery(jobKeys={res.key.job}))
    assert len(res.tasks) == 10

    # No instances should have ancestor id, thereby validating
    # instances created in previous update are not restarted/redeployed
    for task in res.tasks:
        assert task.ancestorId is None


def test__rolling_forward_abort(client):
    """
    Create an update and then abort it
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat.yaml'),
        'create job')
    key = res.key

    # Sleep for sometime to let Peloton transition workflow state
    # from INITIALIZED -> ROLLING_FORWARD or let it be as-is
    time.sleep(5)

    client.abort_job_update(key, 'abort update')
    wait_for_update_status(
        client,
        key,
        {api.JobUpdateStatus.ROLLING_FORWARD},
        api.JobUpdateStatus.ABORTED)


def test__roll_forward_paused_update_abort(client):
    """
    Create an update, pause it and then abort it
    """
    res = client.start_job_update(
        get_job_update_request('test_dc_labrat.yaml'),
        'create job')
    key = res.key

    # Sleep for sometime to let Peloton transition workflow state
    # from INITIALIZED -> ROLLING_FORWARD or let it be as-is
    time.sleep(5)

    client.pause_job_update(key, 'pause update')
    wait_for_update_status(
        client,
        key,
        {api.JobUpdateStatus.ROLLING_FORWARD},
        api.JobUpdateStatus.ROLL_FORWARD_PAUSED)

    client.abort_job_update(key, 'abort update')
    wait_for_update_status(
        client,
        key,
        {api.JobUpdateStatus.ROLLING_FORWARD,
         api.JobUpdateStatus.ROLL_FORWARD_PAUSED},
        api.JobUpdateStatus.ABORTED)


def test__pulsed_update_abort(client):
    """
    Create a pulse update, and then abort it
    """
    req = get_job_update_request('test_dc_labrat_pulsed.yaml')
    res = client.start_job_update(req, 'start pulsed job update test/dc/labrat')
    assert get_update_status(client, res.key) == \
        api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE
    key = res.key

    client.abort_job_update(key, 'abort update')
    wait_for_update_status(
        client,
        key,
        {api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE},
        api.JobUpdateStatus.ABORTED)
