import pytest
import grpc
import time

from peloton_client.pbgen.peloton.api.v0.task import task_pb2
from peloton_client.pbgen.peloton.api.v0.job.job_pb2 import JobConfig

from google.protobuf import json_format

from tests.integration.stateless_job.util import \
    assert_task_config_changed, assert_task_mesos_id_changed, \
    assert_task_config_equal, assert_tasks_failed
from tests.integration.update import Update
from tests.integration.util import load_test_config
from tests.integration.job import Job
from tests.integration.common import IntegrationTestConfig

pytestmark = [pytest.mark.default, pytest.mark.stateless, pytest.mark.update]

UPDATE_STATELESS_JOB_FILE = "test_update_stateless_job.yaml"
UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE = \
    "test_update_stateless_job_add_instances.yaml"
UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE = \
    "test_update_stateless_job_update_and_add_instances.yaml"
UPDATE_STATELESS_JOB_UPDATE_REDUCE_INSTANCES_FILE = \
    "test_stateless_job.yaml"
UPDATE_STATELESS_JOB_BAD_CONFIG_FILE = \
    "test_stateless_job_with_bad_config.yaml"
UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_CONFIG_FILE = \
    "test_stateless_job_successful_health_check_config.yaml"
UPDATE_STATELESS_JOB_BAD_HEALTH_CHECK_FILE = \
    "test_stateless_job_failed_health_check_config.yaml"
INVALID_VERSION_ERR_MESSAGE = 'invalid job configuration version'


def test__create_update(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_add_instances(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5


def test__create_update_update_and_add_instances(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_add_instances_with_batch_size(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5


def test__create_update_update_and_add_instances_with_batch(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_update_restart_jobmgr(stateless_job, jobmgr):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    jobmgr.restart()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_bad_version(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    try:
        update.create(config_version=10)
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert e.details() == INVALID_VERSION_ERR_MESSAGE
        return
    raise Exception("configuration version mismatch error not received")


def test__create_update_stopped_job(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    stateless_job.stop()
    stateless_job.wait_for_state(goal_state='KILLED')
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    stateless_job.start()
    update.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.wait_for_state(goal_state='RUNNING')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_update_stopped_tasks(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    ranges = task_pb2.InstanceRange(to=2)
    setattr(ranges, 'from', 0)
    stateless_job.stop(ranges=[ranges])
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    stateless_job.start()
    stateless_job.wait_for_state(goal_state='RUNNING')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__create_multiple_consecutive_updates(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update1 = Update(stateless_job,
                     updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE)
    update1.create()
    update2 = Update(stateless_job,
                     updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                     batch_size=1)
    update2.create()
    update2.wait_for_state(goal_state='SUCCEEDED')
    update1.wait_for_state(goal_state='ABORTED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert len(old_task_infos) == 3
    assert len(new_task_infos) == 5
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


def test__abort_update(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_AND_ADD_INSTANCES_FILE,
                    batch_size=1)
    update.create()
    update.wait_for_state(goal_state='ROLLING_FORWARD')
    update.abort()
    update.wait_for_state(goal_state='ABORTED')


def test__update_reduce_instances(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    assert len(old_task_infos) == 3
    # first increase instances
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(new_task_infos) == 5
    # now reduce instances
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_REDUCE_INSTANCES_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(new_task_infos) == 3
    # now increase back again
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(new_task_infos) == 5


def test__update_reduce_instances_stopped_tasks(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    assert len(old_task_infos) == 3
    # first increase instances
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_ADD_INSTANCES_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(new_task_infos) == 5
    # now stop last 2 tasks
    ranges = task_pb2.InstanceRange(to=5)
    setattr(ranges, 'from', 3)
    stateless_job.stop(ranges=[ranges])
    # now reduce instance count
    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_UPDATE_REDUCE_INSTANCES_FILE)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(new_task_infos) == 3


# test__create_update_bad_config tests creating an update with bad config
# without rollback
def test__create_update_with_bad_config(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_instance_zero_config = stateless_job.get_task_info(0).config

    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_BAD_CONFIG_FILE,
                    max_failure_instances=3,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='FAILED', failed_state='SUCCEEDED')
    new_instance_zero_config = stateless_job.get_task_info(0).config

    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)
    assert_tasks_failed(stateless_job.list_tasks().value)


# test__create_update_add_instances_with_bad_config
# tests creating an update with bad config and more instances
# without rollback
def test__create_update_add_instances_with_bad_config(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    job_config_dump = load_test_config(UPDATE_STATELESS_JOB_BAD_CONFIG_FILE)
    updated_job_config = JobConfig()
    json_format.ParseDict(job_config_dump, updated_job_config)

    updated_job_config.instanceCount = \
        stateless_job.job_config.instanceCount + 3

    update = Update(stateless_job,
                    batch_size=1,
                    updated_job_config=updated_job_config,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='FAILED', failed_state='SUCCEEDED')

    # only one instance should be added
    assert len(stateless_job.list_tasks().value) == \
        stateless_job.job_config.instanceCount + 1


# test__create_update_reduce_instances_with_bad_config
# tests creating an update with bad config and fewer instances
# without rollback
def test__create_update_reduce_instances_with_bad_config(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value

    job_config_dump = load_test_config(UPDATE_STATELESS_JOB_BAD_CONFIG_FILE)
    updated_job_config = JobConfig()
    json_format.ParseDict(job_config_dump, updated_job_config)

    updated_job_config.instanceCount = \
        stateless_job.job_config.instanceCount - 1

    update = Update(stateless_job,
                    updated_job_config=updated_job_config,
                    batch_size=1,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='FAILED', failed_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    assert len(old_task_infos) == len(new_task_infos)


# test__create_update_with_failed_health_check
# tests an update fails even if the new task state is RUNNING,
# as long as the health check fails
def test__create_update_with_failed_health_check(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')

    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_BAD_HEALTH_CHECK_FILE,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='FAILED', failed_state='SUCCEEDED')


# test__create_update_to_disable_health_check tests an update which
# disables healthCheck
def test__create_update_to_disable_health_check():
    job = Job(job_file=UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_CONFIG_FILE,
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    job.job_config.defaultConfig.healthCheck.enabled = False
    update = Update(job,
                    updated_job_config=job.job_config,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')


# test__create_update_to_enable_health_check tests an update which
# enables healthCheck
def test__create_update_to_enable_health_check():
    job = Job(job_file=UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_CONFIG_FILE,
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.job_config.defaultConfig.healthCheck.enabled = False
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    job.job_config.defaultConfig.healthCheck.enabled = True
    update = Update(job,
                    updated_job_config=job.job_config,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')


# test__create_update_to_unset_health_check tests an update to unset
# health check config
def test__create_update_to_unset_health_check():
    job = Job(job_file=UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_CONFIG_FILE,
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    update = Update(job,
                    updated_job_file=UPDATE_STATELESS_JOB_FILE,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')


# test__create_update_to_unset_health_check tests an update to set
# health check config for a job without health check set
def test__create_update_to_set_health_check():
    job = Job(job_file=UPDATE_STATELESS_JOB_FILE,
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    update = Update(job,
                    updated_job_file=UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_CONFIG_FILE,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')


# test__create_update_to_change_health_check_config tests an update which
# changes healthCheck
def test__create_update_to_change_health_check_config():
    job = Job(job_file=UPDATE_STATELESS_JOB_WITH_HEALTH_CHECK_CONFIG_FILE,
              config=IntegrationTestConfig(max_retry_attempts=100))
    job.job_config.defaultConfig.healthCheck.enabled = False
    job.create()
    job.wait_for_state(goal_state='RUNNING')

    job.job_config.defaultConfig.healthCheck.initialIntervalSecs = 2
    update = Update(job,
                    updated_job_config=job.job_config,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='SUCCEEDED')


# test__auto_rollback_update_with_bad_config tests creating an update with bad config
# with rollback
def test__auto_rollback_update_with_bad_config(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_instance_zero_config = stateless_job.get_task_info(0).config

    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_BAD_CONFIG_FILE,
                    roll_back_on_failure=True,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='ROLLED_BACK')
    new_instance_zero_config = stateless_job.get_task_info(0).config

    assert_task_config_equal(old_instance_zero_config, new_instance_zero_config)


# test__auto_rollback_update_add_instances_with_bad_config
# tests creating an update with bad config and more instances
# with rollback
def test__auto_rollback_update_add_instances_with_bad_config(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_instance_zero_config = stateless_job.get_task_info(0).config

    job_config_dump = load_test_config(UPDATE_STATELESS_JOB_BAD_CONFIG_FILE)
    updated_job_config = JobConfig()
    json_format.ParseDict(job_config_dump, updated_job_config)

    updated_job_config.instanceCount = \
        stateless_job.job_config.instanceCount + 3

    update = Update(stateless_job,
                    updated_job_config=updated_job_config,
                    roll_back_on_failure=True,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='ROLLED_BACK')
    new_instance_zero_config = stateless_job.get_task_info(0).config

    # no instance should be added
    assert len(stateless_job.list_tasks().value) == \
        stateless_job.job_config.instanceCount
    assert_task_config_equal(old_instance_zero_config, new_instance_zero_config)


# test__auto_rollback_update_reduce_instances_with_bad_config
# tests creating an update with bad config and fewer instances
# with rollback
def test__auto_rollback_update_reduce_instances_with_bad_config(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_instance_zero_config = stateless_job.get_task_info(0).config

    job_config_dump = load_test_config(UPDATE_STATELESS_JOB_BAD_CONFIG_FILE)
    updated_job_config = JobConfig()
    json_format.ParseDict(job_config_dump, updated_job_config)

    updated_job_config.instanceCount = \
        stateless_job.job_config.instanceCount - 1

    update = Update(stateless_job,
                    updated_job_config=updated_job_config,
                    roll_back_on_failure=True,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='ROLLED_BACK')
    new_instance_zero_config = stateless_job.get_task_info(0).config

    # no instance should be removed
    assert len(stateless_job.list_tasks().value) == \
        stateless_job.job_config.instanceCount
    assert_task_config_equal(old_instance_zero_config, new_instance_zero_config)


# test__auto_rollback_update_with_failed_health_check
# tests an update fails even if the new task state is RUNNING,
# as long as the health check fails
def test__auto_rollback_update_with_failed_health_check(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_instance_zero_config = stateless_job.get_task_info(0).config

    update = Update(stateless_job,
                    updated_job_file=UPDATE_STATELESS_JOB_BAD_HEALTH_CHECK_FILE,
                    roll_back_on_failure=True,
                    max_failure_instances=1,
                    max_instance_attempts=1)
    update.create()
    update.wait_for_state(goal_state='ROLLED_BACK')
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert_task_config_equal(old_instance_zero_config, new_instance_zero_config)


# test__pause_resume_initialized_update test pause and resume
#  an update in initialized state
def test__pause_resume_initialized_update(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    batch_size=1,
                    updated_job_file=UPDATE_STATELESS_JOB_FILE)
    update.create()
    # immediately pause the update, so the update may still be INITIALIZED
    update.pause()
    update.wait_for_state(goal_state='PAUSED')
    update.resume()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)


# test__pause_resume_initialized_update test pause and resume an update
def test__pause_resume__update(stateless_job):
    stateless_job.create()
    stateless_job.wait_for_state(goal_state='RUNNING')
    old_task_infos = stateless_job.list_tasks().value
    old_instance_zero_config = stateless_job.get_task_info(0).config
    update = Update(stateless_job,
                    batch_size=1,
                    updated_job_file=UPDATE_STATELESS_JOB_FILE)
    update.create()
    # sleep for 1 sec so update can begin to roll forward
    time.sleep(1)
    update.pause()
    update.wait_for_state(goal_state='PAUSED')
    update.resume()
    update.wait_for_state(goal_state='SUCCEEDED')
    new_task_infos = stateless_job.list_tasks().value
    new_instance_zero_config = stateless_job.get_task_info(0).config
    assert_task_mesos_id_changed(old_task_infos, new_task_infos)
    assert_task_config_changed(old_instance_zero_config, new_instance_zero_config)
