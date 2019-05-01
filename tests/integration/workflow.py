import logging
import time

from client import Client
from common import IntegrationTestConfig

from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton
from peloton_client.pbgen.peloton.api.v0.update import update_pb2 as update
from peloton_client.pbgen.peloton.api.v0.update.svc import (
    update_svc_pb2 as update_svc,
)

log = logging.getLogger(__name__)


class Workflow(object):
    """
    Workflow represents a peloton rolling- restart/rolling-start/
    rolling-stop/update workflow
    """

    def __init__(self, workflow_id, client=None, config=None):

        self.workflow_id = workflow_id
        self.config = config or IntegrationTestConfig()
        self.client = client or Client()

    def abort(self):
        """
        aborts the given workflow
        """
        request = update_svc.AbortUpdateRequest(
            updateId=peloton.UpdateID(value=self.workflow_id)
        )
        resp = self.client.update_svc.AbortUpdate(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        return resp

    def wait_for_state(self, goal_state="SUCCEEDED", failed_state="ABORTED"):
        """
        Waits for the workflow to reach a particular state
        :param goal_state: The state to reach
        :param failed_state: The failed state of the update
        """
        state = ""
        attempts = 0
        start = time.time()
        log.info("%s waiting for state %s", self.workflow_id, goal_state)
        state_transition_failure = False
        while attempts < self.config.max_retry_attempts:
            try:
                request = update_svc.GetUpdateRequest(
                    updateId=peloton.UpdateID(value=self.workflow_id)
                )
                resp = self.client.update_svc.GetUpdate(
                    request,
                    metadata=self.client.jobmgr_metadata,
                    timeout=self.config.rpc_timeout_sec,
                )
                update_info = resp.updateInfo
                new_state = update.State.Name(update_info.status.state)
                if state != new_state:
                    log.info(
                        "%s transitioned to state %s",
                        self.workflow_id,
                        new_state,
                    )
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
            log.info(
                "goal_state:%s current_state:%s attempts: %s",
                goal_state,
                state,
                str(attempts),
            )
            assert False

        if attempts == self.config.max_retry_attempts:
            log.info(
                "%s max attempts reached to wait for goal state",
                self.workflow_id,
            )
            log.info("goal_state:%s current_state:%s", goal_state, state)
            assert False

        end = time.time()
        elapsed = end - start
        log.info(
            "%s state transition took %s seconds", self.workflow_id, elapsed
        )
        assert state == goal_state

    def get_state(self):
        """
        get the current state of workflow
        """
        request = update_svc.GetUpdateRequest(
            updateId=peloton.UpdateID(value=self.workflow_id)
        )
        resp = self.client.update_svc.GetUpdate(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        return resp.updateInfo.status.state

    def pause(self):
        """
        pause the given workflow
        """
        request = update_svc.PauseUpdateRequest(
            updateId=peloton.UpdateID(value=self.workflow_id)
        )
        resp = self.client.update_svc.PauseUpdate(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        return resp

    def resume(self):
        """
        resume the given workflow
        """
        request = update_svc.ResumeUpdateRequest(
            updateId=peloton.UpdateID(value=self.workflow_id)
        )
        resp = self.client.update_svc.ResumeUpdate(
            request,
            metadata=self.client.jobmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        return resp
