import logging
import pytest

from tests.integration.stateless_job import (
    StatelessJob,
)
from tests.integration.stateless_job_test.util import (
    assert_pod_id_changed,
)

pytestmark = [
    pytest.mark.k8s,
]

log = logging.getLogger(__name__)


# TODO: refactor existing stateless tests so they run with both mesos and k8s.
def test__restart_killed_job():
    job = StatelessJob(job_file="test_stateless_job_spec_k8s.yaml")
    job.create()
    job.wait_for_state(goal_state="RUNNING")
    old_pod_infos = job.query_pods()

    job.stop()
    job.wait_for_state(goal_state="KILLED")

    job.restart(in_place=False)

    job.wait_for_all_pods_running()

    new_pod_infos = job.query_pods()
    assert_pod_id_changed(old_pod_infos, new_pod_infos)
