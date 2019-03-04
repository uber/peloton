import os
import time
import yaml
import logging

from tests.integration.stateless_job import (
  query_jobs,
)

from google.protobuf import json_format
from tests.integration.common import IntegrationTestConfig
from tests.integration.stateless_job import StatelessJob
from tests.integration.stateless_update import StatelessUpdate
from peloton_client.pbgen.peloton.api.v1alpha.job.stateless import \
   stateless_pb2 as stateless
log = logging.getLogger(__name__)

TEST_CONFIG_DIR = '/test_configs'
RESPOOL_PATH = 'Canary'
RESPOOL_FILE_NAME = 'test_canary_respool.yaml'
MAX_RETRY_ATTEMPTS = 600


def setup_canary_cluster():
    """
    Setup the canary cluster to make sure all jobs are in desired state
    before test start.
    """
    log.info("boostrap canary tests")
    active_jobs = get_active_jobs()
    desired_jobs = get_desired_jobs()
    log.info("bootstraping canary tests:: active_jobs_count: %d, \
      desired_jobs_count: %s", len(active_jobs), len(desired_jobs))

    jobs = patch_jobs(active_jobs, desired_jobs)
    log.info("bootstrap complete for canary test jobs_len: %d", len(jobs))

    return jobs


def get_active_jobs():
    """
    Queries active jobs in the cluster for provided resource pool.
    """
    jobs = {}
    job_list = query_jobs(RESPOOL_PATH)

    for j in job_list:
        jobs[j.get_spec().name] = j
    return jobs


def get_desired_jobs():
    """
    Reads all the job specs for Canary cluster and creates desired job list
    to be running
    """
    jobs = {}
    test_dir = os.path.join(
      os.path.dirname(os.path.abspath(__file__)) + TEST_CONFIG_DIR
    )

    for filename in os.listdir(test_dir):
        jobs[filename.split(".")[0]] = read_job_spec(filename)
    return jobs


def patch_jobs(active_jobs=None, desired_jobs=None):
    """
    patch jobs check current state of the job and applies desired goal state
    for the job. It can yield to create a job or updating a job.
    """
    jobs = {}
    for job_name, job_spec in desired_jobs.items():
        if job_name in active_jobs.keys():
            # job exists -> update to desired state
            patch_job(active_jobs[job_name], job_spec)
            jobs[job_name] = active_jobs[job_name].get_job_id()
        else:
            # job does not exist -> create
            job = StatelessJob(
              job_config=job_spec, config=IntegrationTestConfig(
                  pool_file=RESPOOL_FILE_NAME,
                  max_retry_attempts=MAX_RETRY_ATTEMPTS))
            job.create()
            time.sleep(10)
            job.wait_for_all_pods_running()
            jobs[job_name] = job.get_job_id()

    # TODO: Kill any undesired active job running in the canary cluster

    return jobs


def patch_job(job, job_spec):
    """
    patch one job to desired state
    """
    log.info("patch job_name: %s, job_id: %s", job_spec.name, job.get_job_id())
    update = StatelessUpdate(
      job,
      updated_job_spec=job_spec,
      config=IntegrationTestConfig(
          pool_file=RESPOOL_FILE_NAME,
          max_retry_attempts=MAX_RETRY_ATTEMPTS))
    update.create()
    update.wait_for_state()
    job.wait_for_all_pods_running()


def dump_job_stats(job):
    """
    dumps the jobs stats for a job such as job info, workflow list
    """
    log.info("job_info \n %s", job.get_job())
    # Uncomment once 0.8.1 peloton client is released
    # log.info("list_workflow_infos \n %s", job.list_workflows())


def read_job_spec(filename):
    job_spec_dump = load_test_config(filename)
    job_spec = stateless.JobSpec()
    json_format.ParseDict(job_spec_dump, job_spec)
    return job_spec


def load_test_config(config):
    return load_config(config, TEST_CONFIG_DIR)


def load_config(config, dir=''):
    config_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)) + dir, config
    )
    with open(config_file, "r") as f:
        config = yaml.load(f)
    return config
