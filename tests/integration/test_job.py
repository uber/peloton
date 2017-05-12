import time

from client import Client
from google.protobuf import json_format
from peloton_client.pbgen.peloton.api import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.job import job_pb2 as job
from peloton_client.pbgen.peloton.api.respool import respool_pb2 as respool
from util import load_test_config


sleep_time_sec = 2
max_retry_attempts = 20
respool_root = '/'


def test_create_job():
    client = Client()

    # Create respool if not exists
    respool_config_dump = load_test_config('test_respool.yaml')
    respool_config = respool.ResourcePoolConfig()
    json_format.ParseDict(respool_config_dump, respool_config)
    request = respool.CreateRequest(
        config=respool_config,
    )
    respool_name = request.config.name
    client.respool_svc.create(request)

    request = respool.LookupRequest(
        path=respool.ResourcePoolPath(value=respool_root+respool_name),
    )
    resp = client.respool_svc.lookup(request)
    assert resp.id.value
    assert not resp.error.notFound.message
    assert not resp.error.invalidPath.message
    respool_id = resp.id.value

    # Create job
    job_config_dump = load_test_config('test_job.yaml')
    job_config = job.JobConfig()
    json_format.ParseDict(job_config_dump, job_config)
    job_config.respoolID.value = respool_id
    request = job.CreateRequest(
        config=job_config,
    )
    resp = client.job_svc.create(request)
    assert resp.jobId.value
    job_id = resp.jobId.value
    print 'created job %s' % job_id

    goal_state = 'SUCCEEDED'
    state = ''
    attempts = 0
    while attempts < max_retry_attempts:
        request = job.GetRequest(
            id=peloton.JobID(value=job_id),
        )
        resp = client.job_svc.get(request)
        state = job.JobState.Name(resp.runtime.state)
        if state == goal_state:
            print 'job goal state %s is reached' % goal_state
            break
        print 'current job state %s != goal state %s, retrying' % (
            state,
            goal_state
        )
        time.sleep(sleep_time_sec)
        attempts += 1

    assert state == goal_state
    assert resp.runtime.taskStats[state] == job_config.instanceCount
