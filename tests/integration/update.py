import logging
import grpc

from client import Client
from common import IntegrationTestConfig
from util import load_test_config
from pool import Pool
from workflow import Workflow

from google.protobuf import json_format

from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.v0.update.svc import (
    update_svc_pb2 as update_svc,
)
from peloton_client.pbgen.peloton.api.v0.update import update_pb2 as update
from peloton_client.pbgen.peloton.api.v0.job.job_pb2 import JobConfig

log = logging.getLogger(__name__)

INVALID_VERSION_ERR_MESSAGE = "invalid job configuration version"


class Update(object):
    """
    Update represents a peloton job update
    """

    def __init__(
        self,
        job,
        updated_job_file=None,
        client=None,
        config=None,
        pool=None,
        batch_size=None,
        updated_job_config=None,
        roll_back_on_failure=None,
        max_instance_attempts=None,
        max_failure_instances=None,
        start_paused=None,
    ):

        self.config = config or IntegrationTestConfig()
        self.client = client or Client()
        self.pool = pool or Pool(self.config)
        if updated_job_config is None:
            job_config_dump = load_test_config(updated_job_file)
            updated_job_config = JobConfig()
            json_format.ParseDict(job_config_dump, updated_job_config)
        self.updated_job_config = updated_job_config
        self.batch_size = batch_size or 0
        self.roll_back_on_failure = roll_back_on_failure or False
        self.max_instance_attempts = max_instance_attempts or 0
        self.max_failure_instances = max_failure_instances or 0
        self.start_paused = start_paused or False
        self.job = job
        self.workflow = None

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
            self.updated_job_config.changeLog.version = (
                config_version or job_config_version
            )

            request = update_svc.CreateUpdateRequest(
                jobId=peloton.JobID(value=self.job.job_id),
                jobConfig=self.updated_job_config,
                updateConfig=update.UpdateConfig(
                    batchSize=self.batch_size,
                    rollbackOnFailure=self.roll_back_on_failure,
                    maxInstanceAttempts=self.max_instance_attempts,
                    maxFailureInstances=self.max_failure_instances,
                    startPaused=self.start_paused,
                ),
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
                if (
                    e.code() == grpc.StatusCode.ABORTED
                    and e.details() == INVALID_VERSION_ERR_MESSAGE
                    and config_version is None
                ):
                    continue
                raise
            break

        assert resp.updateID.value
        self.workflow = Workflow(
            resp.updateID.value, client=self.client, config=self.config
        )
        log.info("created update %s", self.workflow.workflow_id)

    def abort(self):
        """
        aborts the given update
        """
        return self.workflow.abort()

    def wait_for_state(self, goal_state="SUCCEEDED", failed_state="ABORTED"):
        """
        Waits for the update to reach a particular state
        :param goal_state: The state to reach
        :param failed_state: The failed state of the update
        """
        self.workflow.wait_for_state(
            goal_state=goal_state, failed_state=failed_state
        )

    def pause(self):
        """
        pause the given update
        """
        return self.workflow.pause()

    def resume(self):
        """
        resume the given update
        """
        return self.workflow.resume()
