import logging
import time
import grpc

from client import Client
from job import IntegrationTestConfig
from util import load_test_config
from pool import Pool

from google.protobuf import json_format

from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.v0.update.svc import \
    update_svc_pb2 as update_svc
from peloton_client.pbgen.peloton.api.v0.update import update_pb2 as update
from peloton_client.pbgen.peloton.api.v0.job.job_pb2 import JobConfig


log = logging.getLogger(__name__)

INVALID_VERSION_ERR_MESSAGE = 'invalid job configuration version'


class Update(object):
    """
    Update represents a peloton job update
    """
    def __init__(self, job,
                 updated_job_file=None,
                 client=None,
                 config=None,
                 pool=None,
                 batch_size=None,
                 updated_job_config=None):

        self.update_id = None
        self.config = config or IntegrationTestConfig()
        self.client = client or Client()
        self.pool = pool or Pool(self.config)
        if updated_job_config is None:
            job_config_dump = load_test_config(updated_job_file)
            updated_job_config = JobConfig()
            json_format.ParseDict(job_config_dump, updated_job_config)
        self.updated_job_config = updated_job_config
        self.batch_size = batch_size or 0
        self.job = job

    def create(self, config_version=None):
        """
        creates an update based on the job and config.
        if config_version is provided, create will use the provided value,
        and raise an exception if version is wrong.
        if config_version is provided, create will query job runtime to
        get config version and retry until version is correct.
        :return: the update ID
        """
        respool_id = self.pool.ensure_exists()
        self.updated_job_config.respoolID.value = respool_id

        while True:
            job_config_version = self.job.get_runtime().configurationVersion
            self.updated_job_config.changeLog.version = config_version or \
                job_config_version

            request = update_svc.CreateUpdateRequest(
                jobId=peloton.JobID(value=self.job.job_id),
                jobConfig=self.updated_job_config,
                updateConfig=update.UpdateConfig(
                    batchSize=self.batch_size
                )
            )
            try:
                resp = self.client.update_svc.CreateUpdate(
                    request,
                    metadata=self.client.jobmgr_metadata,
                    timeout=self.config.rpc_timeout_sec,
                )
            except grpc.RpcError as e:
                # if config version is incorrect and caller does not specify a
                # config version, get config version from job runtime
                # and try again.
                if e.code() == grpc.StatusCode.INVALID_ARGUMENT \
                        and e.details() == INVALID_VERSION_ERR_MESSAGE \
                        and config_version is None:
                    continue
                raise
            break

        assert resp.updateID.value
        self.update_id = resp.updateID.value
        log.info('created update %s', self.update_id)

    def abort(self):
        """
        aborts the given update
        """
        request = update_svc.AbortUpdateRequest(
            updateId=peloton.UpdateID(value=self.update_id),
        )
        resp = self.client.update_svc.AbortUpdate(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        return resp

    def wait_for_state(self, goal_state='SUCCEEDED', failed_state='ABORTED'):
        """
        Waits for the update to reach a particular state
        :param goal_state: The state to reach
        :param failed_state: The failed state of the update
        """
        state = ''
        attempts = 0
        start = time.time()
        log.info('%s waiting for state %s', self.update_id, goal_state)
        state_transition_failure = False
        while attempts < self.config.max_retry_attempts:
            try:
                request = update_svc.GetUpdateRequest(
                    updateId=peloton.UpdateID(value=self.update_id),
                )
                resp = self.client.update_svc.GetUpdate(
                    request,
                    metadata=self.client.jobmgr_metadata,
                    timeout=self.config.rpc_timeout_sec,
                )
                update_info = resp.updateInfo
                new_state = update.State.Name(update_info.status.state)
                if state != new_state:
                    log.info('%s transitioned to state %s', self.update_id,
                             new_state)
                state = new_state
                if state == goal_state:
                    break
                # If we assert here, we will log the exception,
                # and continue with the finally block. Set a flag
                # here to indicate failure and then break the loop
                # in the finally block
                if state == failed_state:
                    state_transition_failure = True
            except Exception as e:
                log.warn(e)
            finally:
                if state_transition_failure:
                    break
                time.sleep(self.config.sleep_time_sec)
                attempts += 1

        if state_transition_failure:
            log.info('goal_state:%s current_state:%s attempts: %s',
                     goal_state, state, str(attempts))
            assert False

        if attempts == self.config.max_retry_attempts:
            log.info('%s max attempts reached to wait for goal state',
                     self.update_id)
            log.info('goal_state:%s current_state:%s', goal_state, state)
            assert False

        end = time.time()
        elapsed = end - start
        log.info('%s state transition took %s seconds',
                 self.update_id, elapsed)
        assert state == goal_state
