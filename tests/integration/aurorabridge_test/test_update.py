import pytest

from tests.integration.aurorabridge_test.client import Client, api

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]


@pytest.mark.skip(reason='resource pool bootstrapping disabled')
def test__start_job_update():
    # This is basically just a simple smoketest to ensure aurorabridge is wired
    # up correctly.
    c = Client()
    res = c.start_job_update(api.JobUpdateRequest(
        taskConfig=api.TaskConfig(
            job=api.JobKey(
                role='test-role',
                environment='test-environment',
                name='test-name',
            ),
            resources=[api.Resource(numCpus=1)],
        ),
    ), 'some message')
    assert res.responseCode == api.ResponseCode.OK
