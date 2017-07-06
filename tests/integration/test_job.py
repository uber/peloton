import time

from client import Client
from google.protobuf import json_format
from peloton_client.pbgen.peloton.api import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.job import job_pb2 as job
from peloton_client.pbgen.peloton.api.respool import respool_pb2 as respool
from util import load_test_config


respool_root = '/'


class IntegrationTestConfig(object):
    def __init__(self, pool_file='test_respool.yaml', job_file='test_job.yaml',
                 max_retry_attempts=20, sleep_time_sec=2):
        respool_config_dump = load_test_config(pool_file)
        respool_config = respool.ResourcePoolConfig()
        json_format.ParseDict(respool_config_dump, respool_config)
        self.respool_config = respool_config

        job_config_dump = load_test_config(job_file)
        job_config = job.JobConfig()
        json_format.ParseDict(job_config_dump, job_config)
        self.job_config = job_config

        self.max_retry_attempts = max_retry_attempts
        self.sleep_time_sec = sleep_time_sec


def format_stats(stats):
    result = ''
    for name in job.JobState.keys():
        result += '%s: %s ' % (name.lower(), stats[name])
    return result


def create_job(config=None):
    if config is None:
        config = IntegrationTestConfig()

    client = Client()

    # Create respool if not exists
    request = respool.CreateRequest(
        config=config.respool_config,
    )
    respool_name = request.config.name
    client.respool_svc.create(request)

    request = respool.LookupRequest(
        path=respool.ResourcePoolPath(value=respool_root + respool_name),
    )
    resp = client.respool_svc.lookup(request)
    assert resp.id.value
    assert not resp.error.notFound.message
    assert not resp.error.invalidPath.message
    respool_id = resp.id.value

    # Create job
    config.job_config.respoolID.value = respool_id
    request = job.CreateRequest(
        config=config.job_config,
    )
    resp = client.job_svc.create(request)
    start = time.time()
    assert resp.jobId.value
    job_id = resp.jobId.value
    print 'created job %s' % job_id

    goal_state = 'SUCCEEDED'
    state = ''
    attempts = 0
    while attempts < config.max_retry_attempts:
        request = job.GetRequest(
            id=peloton.JobID(value=job_id),
        )
        resp = client.job_svc.get(request)
        runtime = resp.jobInfo.runtime
        state = job.JobState.Name(runtime.state)
        if state == goal_state:
            print 'job goal state %s is reached' % goal_state
            break
        print format_stats(runtime.taskStats)
        print 'current job state %s != goal state %s, retrying' % (
            state,
            goal_state
        )
        time.sleep(config.sleep_time_sec)
        attempts += 1

    end = time.time()
    elapsed = end - start
    print 'total elapsed time was %s seconds' % elapsed
    assert state == goal_state
    assert runtime.taskStats[state] == config.job_config.instanceCount


def test_create_batch_job():
    config = IntegrationTestConfig(max_retry_attempts=100, sleep_time_sec=10)
    config.job_config.instanceCount = 1000
    # We want maximal concurrency, so we remove the maximumRunningInstances
    # limit
    config.job_config.sla.maximumRunningInstances = 0
    config.job_config.defaultConfig.command.value = 'echo batch lol'
    # We do not want two instances to run a different command, as in the
    # standard job
    del config.job_config.instanceConfig[0]
    del config.job_config.instanceConfig[1]
    create_job(config)


def test_create_job():
    create_job()
