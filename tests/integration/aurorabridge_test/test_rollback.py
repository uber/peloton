import pytest
import time

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    get_job_update_request,
    start_job_update,
    wait_for_rolled_back,
)

# disable auto rollback given its flaky behavior
pytestmark = [pytest.mark.aurorabridge,
              pytest.mark.random_order(disabled=True)]


def test__simple_auto_rolled_back(client):
    """
    Create a job, then issue a bad config update and validate
    job is rolled back to previous version
    """
    start_job_update(
        client,
        'test_dc_labrat.yaml',
        'start job update test/dc/labrat')

    # Add some wait time for lucene index to build
    time.sleep(10)

    res = client.start_job_update(
        get_job_update_request('test_dc_labrat_bad_config.yaml'),
        'rollout bad config')
    wait_for_rolled_back(client, res.key)

    # validate job is rolled back to previous config
    res = client.get_tasks_without_configs(api.TaskQuery(
        jobKeys={res.key.job},
        statuses={api.ScheduleStatus.RUNNING}
    ))

    tasks = res.tasks
    assert len(tasks) == 3

    for t in tasks:
        for r in t.assignedTask.task.resources:
            if r.numCpus > 0:
                assert r.numCpus == 0.25
            elif r.ramMb > 0:
                assert r.ramMb == 128
            elif r.diskMb > 0:
                assert r.diskMb == 128
            else:
                assert False, 'unexpected resource {}'.format(r)
