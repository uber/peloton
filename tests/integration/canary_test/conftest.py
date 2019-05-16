import pytest
import time
import random
import logging
import json
import os
from filelock import FileLock

from tests.integration.canary_test.util import (
    setup_canary_cluster,
    patch_job,
    dump_job_stats,
    read_job_spec,
    RESPOOL_FILE_NAME,
    MAX_RETRY_ATTEMPTS,
)
from tests.integration.common import IntegrationTestConfig
from tests.integration.stateless_job import StatelessJob

log = logging.getLogger(__name__)

# TEST_JOB_MAP_FILE contains the a map of which jobs are executed for this test.
# This prevents from re-running same job & test match.
TEST_JOB_MAP_FILE = "test_job_map.json"

# JOB_IN_USE_FILE contains jobs currently used by running tests, it is to prevent
# to test to select same job.
JOB_IN_USE_FILE = "job_in_use.json"

# JOBS contains raw dump for desired jobs running on cluster.
JOBS = "jobs.json"

# FILE_LOCK is to syncronize multiple test runner processes for job distribution
FILE_LOCK = FileLock("file.lock", timeout=600)

# jobs (dic):: job_name -> job_id
pytest.jobs = {}
# test_job_map (dic):: test_name -> job_name
pytest.test_job_map = {}
# job_in_use (dic):: job_name -> bool (True: in_use)
pytest.job_in_use = {}


@pytest.fixture(scope="module", autouse=True)
def canary_tester(request):
    """
    Bootstraps the canary cluster, by making sure jobs are in
    desired state before tests run.
    """
    FILE_LOCK.acquire()
    try:
        if not os.path.exists(JOBS):
            pytest.jobs = setup_canary_cluster()
            write_to_file(JOBS, pytest.jobs)
        else:
            pytest.jobs = read_from_file(JOBS)
    finally:
        FILE_LOCK.release()

    yield

    # cleanup metadata files
    FILE_LOCK.acquire()
    try:
        if (
            os.path.exists(JOB_IN_USE_FILE)
            and len(read_from_file(JOB_IN_USE_FILE)) == 0
        ):
            os.remove(TEST_JOB_MAP_FILE)
            os.remove(JOB_IN_USE_FILE)
            os.remove(JOBS)
            log.info("removed metadata")
    except Exception as e:
        log.info("error on canary setup cleanup: %s", e)
    finally:
        FILE_LOCK.release()


@pytest.fixture(scope="module", autouse=True)
def setup_cluster(request):
    """
    override parent module fixture
    """
    pass


@pytest.fixture
def canary_job(request):
    """
    returns a unique job for the test, dumps job stats on test failure
    and restores job's original state after test run
    """
    job = get_unique_job(request)
    job_name = job.get_spec().name
    yield job

    FILE_LOCK.acquire()
    try:
        pytest.job_in_use = read_from_file(JOB_IN_USE_FILE)
        del pytest.job_in_use[job_name]
        write_to_file(JOB_IN_USE_FILE, pytest.job_in_use)

        # Test failed, dump job stats
        if request.node.rep_call.failed:
            log.info("failed test: %s", request.node.name)
            dump_job_stats(job)
        else:
            log.info('restore job: %s, job_id: %s, after test: %s',
                     job_name, job.job_id, request.node.name)
            patch_job(job, read_job_spec(job_name+'.yaml'))
    finally:
        FILE_LOCK.release()


def pytest_runtest_setup(item):
    """
    fail subsequent tests if previous test failed
    """
    previousfailed = getattr(item.parent, "_previousfailed", None)
    if previousfailed is not None:
        pytest.xfail("previous test failed (%s)" % previousfailed.name)


def get_unique_job(request):
    """
    Finds a unique job to run for a test.
    Job selected in random across multiple test suite runs.
    """
    while True:
        job = None
        FILE_LOCK.acquire()
        try:
            pytest.test_job_map = read_from_file(TEST_JOB_MAP_FILE)
            pytest.job_in_use = read_from_file(JOB_IN_USE_FILE)

            job_list = list(pytest.jobs.keys())
            random.shuffle(job_list)

            for j in job_list:
                test_name = request.node.name
                id = test_name.split("[")[0] + "_" + j

                # check if test && job are already matched
                if id in pytest.test_job_map or j in pytest.job_in_use:
                    continue

                pytest.test_job_map[id] = ""
                pytest.job_in_use[j] = ""

                write_to_file(TEST_JOB_MAP_FILE, pytest.test_job_map)
                write_to_file(JOB_IN_USE_FILE, pytest.job_in_use)

                log.info(
                    "test_job_mapping:: test_name: %s, map_id: %s",
                    test_name,
                    id,
                )

                # create deep copy for job
                job = StatelessJob(
                    job_id=pytest.jobs[j],
                    config=IntegrationTestConfig(
                        pool_file=RESPOOL_FILE_NAME,
                        max_retry_attempts=MAX_RETRY_ATTEMPTS,
                    ),
                )
                break
        finally:
            FILE_LOCK.release()

        if job is not None:
            break
        time.sleep(10)

    return job


def read_from_file(file_name):
    """
    reads json dump from the file, if the file is empty then job load
    returns error, thereby function returns empty map.
    """
    FILE_LOCK.acquire()
    try:
        with open(file_name, "r") as file:
            return json.load(file)
    except (IOError, ValueError):
        return {}
    finally:
        FILE_LOCK.release()


def write_to_file(file_name, data):
    """
    overwrites the file with json dump provided as argument.
    on error, it passes it.
    """
    FILE_LOCK.acquire()
    try:
        with open(file_name, "w+") as file:
            try:
                file.write(json.dumps(data))
            except ValueError as e:
                log.info(
                    "error occurred on writing to file: %s, data: %s, \
                         error: %s",
                    file_name,
                    data,
                    e,
                )
                raise
    finally:
        FILE_LOCK.release()
