import os
import time
import yaml
import logging
import pytest

from tests.integration.stateless_job import get_job_from_job_name

from google.protobuf import json_format
from tests.integration.common import IntegrationTestConfig
from tests.integration.stateless_job import StatelessJob
from tests.integration.stateless_update import StatelessUpdate
from peloton_client.pbgen.peloton.api.v1alpha.job.stateless import (
    stateless_pb2 as stateless,
)

log = logging.getLogger(__name__)

TEST_CONFIG_DIR = "/test_configs"
RESPOOL_PATH = "Canary"
RESPOOL_FILE_NAME = "test_canary_respool.yaml"
MAX_RETRY_ATTEMPTS = 600


def setup_canary_cluster():
    """
    Setup the canary cluster to make sure all jobs are in desired state
    before test start.
    """
    log.info("boostrap canary tests")
    desired_jobs = get_desired_jobs()
    active_jobs = get_active_jobs(desired_jobs)

    log.info(
        "bootstraping canary tests:: active_jobs_count: %d, \
      desired_jobs_count: %s",
        len(active_jobs),
        len(desired_jobs),
    )

    jobs = patch_jobs(active_jobs, desired_jobs)
    log.info("bootstrap complete for canary test jobs_len: %d", len(jobs))

    return jobs


def get_active_jobs(desired_jobs):
    """
    Get active jobs in the cluster for provided resource pool.
    """
    jobs = {}
    job_list = []
    for job_name in desired_jobs.keys():
        job = get_job_from_job_name(job_name)
        if job is not None:
            job_list.append(job)

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
            j = active_jobs[job_name]

            # failfast is not None then do not run canary test
            # until dirty jobs are restored manually.
            if os.getenv("FAILFAST") == "NO":
                # job exists -> update to desired state
                patch_job(j, job_spec)
                jobs[job_name] = j.get_job_id()
            else:
                # if job update diff has non-nil result means that previous
                # canary test run failed and we want more runs to block
                # until issue is manually debugged and state is restored.
                job_spec.respool_id.value = j.get_spec().respool_id.value
                resp = j.get_replace_job_diff(job_spec=job_spec)
                print resp
                if len(resp.instances_removed) > 0 or \
                   len(resp.instances_updated) > 0 or \
                   len(resp.instances_added) > 0:
                    pytest.exit(
                        "canary test run was aborted, since jobs are dirty!!")

                jobs[job_name] = j.get_job_id()
        else:
            # job does not exist -> create
            job = StatelessJob(
                job_config=job_spec,
                config=IntegrationTestConfig(
                    pool_file=RESPOOL_FILE_NAME,
                    max_retry_attempts=MAX_RETRY_ATTEMPTS,
                ),
            )
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
            pool_file=RESPOOL_FILE_NAME, max_retry_attempts=MAX_RETRY_ATTEMPTS
        ),
    )
    update.create()
    update.wait_for_state()
    job.wait_for_all_pods_running()


def dump_job_stats(job):
    """
    dumps the jobs stats for a job such as job info, workflow list
    """
    log.info("job_info \n %s", job.get_job())
    log.info("list_workflow_infos \n %s", job.list_workflows())


def read_job_spec(filename):
    job_spec_dump = load_test_config(filename)
    job_spec = stateless.JobSpec()
    json_format.ParseDict(job_spec_dump, job_spec)
    return job_spec


def load_test_config(config):
    return load_config(config, TEST_CONFIG_DIR)


def load_config(config, dir=""):
    config_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)) + dir, config
    )
    with open(config_file, "r") as f:
        config = yaml.load(f)
    return config
