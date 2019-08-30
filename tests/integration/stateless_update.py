import logging
import grpc

from client import Client
from common import IntegrationTestConfig
from util import load_test_config
from pool import Pool

from google.protobuf import json_format

from peloton_client.pbgen.peloton.api.v1alpha.job.stateless.stateless_pb2 import (
    JobSpec,
)
from peloton_client.pbgen.peloton.api.v1alpha.job.stateless.svc import (
    stateless_svc_pb2 as stateless_svc,
)
from peloton_client.pbgen.peloton.api.v1alpha import (
    peloton_pb2 as v1alpha_peloton,
)
from peloton_client.pbgen.peloton.api.v1alpha.job.stateless import (
    stateless_pb2 as stateless,
)
from stateless_job import INVALID_ENTITY_VERSION_ERR_MESSAGE

log = logging.getLogger(__name__)


class StatelessUpdate(object):
    """
    StatelessUpdate represents a peloton stateless job update
    """

    def __init__(
        self,
        job,
        updated_job_file=None,
        client=None,
        config=None,
        pool=None,
        batch_size=None,
        updated_job_spec=None,
        roll_back_on_failure=None,
        max_instance_attempts=None,
        max_failure_instances=None,
        start_paused=None,
    ):

        self.config = config or IntegrationTestConfig(
            pool_file='test_stateless_respool.yaml')
        self.client = client or Client()
        self.pool = pool or Pool(self.config, self.client)
        if updated_job_spec is None:
            job_config_dump = load_test_config(updated_job_file)
            updated_job_spec = JobSpec()
            json_format.ParseDict(job_config_dump, updated_job_spec)
        self.updated_job_spec = updated_job_spec
        self.batch_size = batch_size or 0
        self.roll_back_on_failure = roll_back_on_failure or False
        self.max_instance_attempts = max_instance_attempts or 0
        self.max_failure_instances = max_failure_instances or 0
        self.start_paused = start_paused or False
        self.job = job

    def create(self, in_place=False, entity_version=None):
        """
        replace the job spec with the spec provided in StatelessUpdate
        if entity_version is provided,  replace will use the provided value,
        and raise an exception if version is wrong.
        if entity_version is not provided, replace will query job runtime to
        get config version and retry until version is correct.
        :return: the update ID
        """
        # wait for job manager leader
        self.job.wait_for_jobmgr_available()

        respool_id = self.pool.ensure_exists()
        self.updated_job_spec.respool_id.value = respool_id

        job_entity_version = (
            entity_version
            or self.job.entity_version
            or self.job.get_status().version.value
        )

        while True:
            request = stateless_svc.ReplaceJobRequest(
                job_id=v1alpha_peloton.JobID(value=self.job.job_id),
                version=v1alpha_peloton.EntityVersion(
                    value=job_entity_version
                ),
                spec=self.updated_job_spec,
                update_spec=stateless.UpdateSpec(
                    batch_size=self.batch_size,
                    rollback_on_failure=self.roll_back_on_failure,
                    max_instance_retries=self.max_instance_attempts,
                    max_tolerable_instance_failures=self.max_failure_instances,
                    start_paused=self.start_paused,
                    in_place=in_place,
                ),
            )
            try:
                resp = self.client.stateless_svc.ReplaceJob(
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
                    and INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
                    and entity_version is None
                ):
                    job_entity_version = (
                        entity_version or self.job.get_status().version.value
                    )
                    continue
                raise
            break
        self.job.entity_version = resp.version.value
        log.info(
            "job spec replaced with new entity version: %s",
            self.job.entity_version,
        )

    def abort(self, entity_version=None):
        """
        aborts the given update
        """
        # wait for job manager leader
        self.job.wait_for_jobmgr_available()

        job_entity_version = (
            entity_version
            or self.job.entity_version
            or self.job.get_status().version.value
        )

        while True:
            request = stateless_svc.AbortJobWorkflowRequest(
                job_id=v1alpha_peloton.JobID(value=self.job.job_id),
                version=v1alpha_peloton.EntityVersion(
                    value=job_entity_version
                ),
            )
            try:
                resp = self.client.stateless_svc.AbortJobWorkflow(
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
                    and INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
                    and entity_version is None
                ):
                    job_entity_version = (
                        entity_version or self.job.get_status().version.value
                    )
                    continue
                raise
            break
        self.job.entity_version = resp.version.value
        log.info("job workflow aborted: %s", self.job.entity_version)

    def wait_for_state(self, goal_state="SUCCEEDED", failed_state="ABORTED"):
        """
        Waits for the update to reach a particular state
        :param goal_state: The state to reach
        :param failed_state: The failed state of the update
        """
        self.job.wait_for_workflow_state(
            goal_state=goal_state, failed_state=failed_state
        )

    def pause(self, entity_version=None):
        """
        pause the given update
        """
        # wait for job manager leader
        self.job.wait_for_jobmgr_available()

        job_entity_version = (
            entity_version
            or self.job.entity_version
            or self.job.get_status().version.value
        )

        while True:
            request = stateless_svc.PauseJobWorkflowRequest(
                job_id=v1alpha_peloton.JobID(value=self.job.job_id),
                version=v1alpha_peloton.EntityVersion(
                    value=job_entity_version
                ),
            )
            try:
                resp = self.client.stateless_svc.PauseJobWorkflow(
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
                    and INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
                    and entity_version is None
                ):
                    job_entity_version = (
                        entity_version or self.job.get_status().version.value
                    )
                    continue
                raise
            break
        self.job.entity_version = resp.version.value
        log.info("job workflow paused: %s", self.job.entity_version)

    def resume(self, entity_version=None):
        """
        resume the given update
        """
        # wait for job manager leader
        self.job.wait_for_jobmgr_available()

        job_entity_version = (
            entity_version
            or self.job.entity_version
            or self.job.get_status().version.value
        )

        while True:
            request = stateless_svc.ResumeJobWorkflowRequest(
                job_id=v1alpha_peloton.JobID(value=self.job.job_id),
                version=v1alpha_peloton.EntityVersion(
                    value=job_entity_version
                ),
            )
            try:
                resp = self.client.stateless_svc.ResumeJobWorkflow(
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
                    and INVALID_ENTITY_VERSION_ERR_MESSAGE in e.details()
                    and entity_version is None
                ):
                    job_entity_version = (
                        entity_version or self.job.get_status().version.value
                    )
                    continue
                raise
            break
        self.job.entity_version = resp.version.value
        log.info("job workflow resumed: %s", self.job.entity_version)
