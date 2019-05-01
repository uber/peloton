import pytest

from tests.integration.aurorabridge_test.util import (
    start_job_update,
    get_mesos_maser_state,
)

pytestmark = [pytest.mark.default, pytest.mark.aurorabridge]


def test__mesos_task_label(client):
    # verify aurora metadata is correctly populated in mesos task level
    start_job_update(
        client,
        "test_dc_labrat_uns.yaml",
        "start job update test/dc/labrat_uns",
    )

    state = get_mesos_maser_state()
    assert len(state["frameworks"]) == 1
    assert state["frameworks"][0]["name"] == "Peloton"

    framework = state["frameworks"][0]
    assert len(framework["tasks"]) == 1

    task = framework["tasks"][0]
    assert len(task["labels"]) > 0

    for l in task["labels"]:
        if l["key"] == "org.apache.aurora.metadata.uns":
            break
    else:
        assert False, "expected label not found"
