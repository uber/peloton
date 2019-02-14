import pytest

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    check_response_ok,
    gen_job_key,
    wait_for_rolled_forward,
)

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]


def test__start_job_update_rolled_forward(client):
    res = client.start_job_update(api.JobUpdateRequest(
        taskConfig=api.TaskConfig(
            job=gen_job_key(),
            resources=[api.Resource(numCpus=1)],
        ),
    ), 'some message')
    check_response_ok(res)

    wait_for_rolled_forward(client, res.result.startJobUpdateResult.key)
