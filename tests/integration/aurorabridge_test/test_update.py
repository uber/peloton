import pytest

from tests.integration.aurorabridge_test.client import api
from tests.integration.aurorabridge_test.util import (
    check_response_ok,
    get_job_update_request,
    get_update_status,
    start_job_update,
    wait_for_update_status,
)

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]


def test__start_job_update_rolled_forward(client):
    start_job_update(
        client,
        'test_dc_labrat.yaml',
        'start job update test/dc/labrat',
    )


def test__start_job_update_with_pulse(client):
    req = get_job_update_request('test_dc_labrat_pulsed.yaml')
    res = client.start_job_update(req, 'start pulsed job update test/dc/labrat')
    check_response_ok(res)
    key = res.result.startJobUpdateResult.key
    assert get_update_status(client, key) == \
        api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE

    res = client.pulse_job_update(key)
    check_response_ok(res)
    wait_for_update_status(
        client,
        key,
        {
            api.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE,
            api.JobUpdateStatus.ROLLING_FORWARD,
        },
        api.JobUpdateStatus.ROLLED_FORWARD)
