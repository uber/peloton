import random
import string
import time

from tests.integration.aurorabridge_test.client import api


def _gen_random_str(length):
    return ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in range(length))


def gen_job_key():
    return api.JobKey(
        role='test-role-%s' % _gen_random_str(6),
        environment='test-env-%s' % _gen_random_str(6),
        name='test-name-%s' % _gen_random_str(6))


def check_response_ok(response):
    assert response.responseCode == api.ResponseCode.OK, \
        'bad response: {code} {msg}'.format(
            code=api.ResponseCode.name_of(response.responseCode),
            msg=','.join(map(lambda d: d.message, response.details)))


def wait_for_rolled_forward(client, job_update_key):
    wait_for_update_status(
        client,
        job_update_key,
        set([api.JobUpdateStatus.ROLLING_FORWARD]),
        api.JobUpdateStatus.ROLLED_FORWARD)


def wait_for_update_status(
        client,
        job_update_key,
        allowed_intermediate_statuses,
        status,
        timeout_secs=240):

    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        latest = get_update_status(client, job_update_key)
        if latest == status:
            return
        assert latest in allowed_intermediate_statuses
        time.sleep(2)

    assert False, 'timed out waiting for {status}, last status: {latest}'.format(
        status=api.JobUpdateStatus.name_of(status),
        latest=api.JobUpdateStatus.name_of(latest))


def get_update_status(client, job_update_key):
    res = client.get_job_update_summaries(api.JobUpdateQuery(key=job_update_key))
    check_response_ok(res)

    summaries = res.result.getJobUpdateSummariesResult.updateSummaries
    assert len(summaries) == 1

    summary = summaries[0]
    assert summary.key == job_update_key

    return summary.state.status
