import pytest

from tests.integration.aurorabridge_test.util import (
    check_response_ok,
    wait_for_rolled_forward,
    get_job_update_request,
)

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]


def test__start_job_update_rolled_forward(client):
    res = client.start_job_update(get_job_update_request("test_dc_labrat.yaml"),
                                  'some message')
    check_response_ok(res)

    wait_for_rolled_forward(client, res.result.startJobUpdateResult.key)
