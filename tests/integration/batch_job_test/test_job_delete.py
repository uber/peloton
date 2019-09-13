import pytest
import logging
import grpc

from peloton_client.pbgen.peloton.api.v0.job import job_pb2
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton

log = logging.getLogger(__name__)

# Mark test module so that we can run tests by tags
pytestmark = [
    pytest.mark.default,
    pytest.mark.jobdelete,
    pytest.mark.random_order(disabled=True),
]


# test job delete API and try to delete a active job, this should fail
def test__delete_active_job(jobs_by_state, peloton_client):
    job = jobs_by_state[1]["RUNNING"][0]
    job.create()
    job.wait_for_state(goal_state="RUNNING")

    client = peloton_client
    request = job_pb2.DeleteRequest(id=peloton.JobID(value=job.job_id))
    failed = True
    try:
        client.job_svc.Delete(
            request, metadata=client.jobmgr_metadata, timeout=10
        )
        failed = False
    except grpc.RpcError as e:
        log.info(e)
        errmsg = "Job is not in a terminal state"
        assert errmsg in e.details()
        assert e.code() is grpc.StatusCode.INTERNAL
    job.stop()
    job.wait_for_state(goal_state="KILLED")
    assert failed is True


# test job delete API to delete a completed job
def test__delete_completed_job(jobs_by_state, peloton_client):

    job = jobs_by_state[1]["SUCCEEDED"][0]
    job.create()
    job.wait_for_state()

    client = peloton_client
    request = job_pb2.DeleteRequest(id=peloton.JobID(value=job.job_id))
    try:
        client.job_svc.Delete(
            request, metadata=client.jobmgr_metadata, timeout=10
        )
    except grpc.RpcError as e:
        log.info(e)
        assert e is None


# test delete job API with a invalid job id, this should fail
def test__delete_non_existing_job(peloton_client):
    client = peloton_client
    request = job_pb2.DeleteRequest(
        id=peloton.JobID(value="00010203-0405-0607-0809-0a0b0c0d0e0f")
    )
    failed = True
    try:
        client.job_svc.Delete(
            request, metadata=client.jobmgr_metadata, timeout=10
        )
        failed = False
    except grpc.RpcError as e:
        log.info(e)
        assert e.details() == "job not found"
        assert e.code() is grpc.StatusCode.NOT_FOUND
    assert failed is True
