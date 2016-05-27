/**
 *  Upgrade Workflow Manager Thrift interface
 */

namespace java com.uber.infra.peloton

include "common.thrift"
include "job.thrift"

typedef common.EntityID WorkflowID


/**
 *  Batch size config for a job rolling upgrade
 */
struct UpgradeBatchConfig
{
  /** Upgrade batch size of the deployment */
  1: optional i32 batchSize

  /**
   *  Upgrade batch percentage of the deployment. If present,
   *  will take precedence over batchSize
   */
  2: optional double batchPercentage

  /** Whether or not to stop all instance before upgrade */
  3: optional bool stopBeforeUpgrade
}


/**
 *  Configuration spec of an upgrade workflow
 */
struct UpgradeWorkflowSpec
{
  /** Entity id of the job to be upgraded */
  1: required job.JobID jobId

  /**
   *  New configuration of the job to be upgraded. The new job config
   *  will be applied to all instances in a rolling upgrade fashion without
   *  voilating the job SLA
   */
  2: required job.JobConfig jobConfig

  /** The batch size config of the rolling upgrade */
  3: required UpgradeBatchConfig batchConfig
}


/**
 *  Runtime state of a job upgrade workflow
 */
enum UpgradeWorkflowState {
  /** The upgrade workflow is rolling forward */
  ROLLING_FORWARD = 0

  /** The upgrade workflow is rolling back */
  ROLLING_BACK    = 1

  /** The upgrade workflow is paused */
  PAUSED          = 2

  /** The upgrade workflow completed successfully */
  SUCCEEDED       = 3

  /** The upgrade workflow is rolled back */
  ROLLED_BACK     = 4

  /** The upgrade workflow is aborted  */
  ABORTED         = 3
}


/**
 *  Runtime info of an upgrade workflow
 */
struct UpgradeWorkflowInfo
{
  /** Entity Id of the upgrade workflow */
  1: required WorkflowID workflowId

  /** Number of instances that have been upgraded */
  2: required i32 numInstancesDone

  /** Number of instances to be upgraded */
  3: required i32 numInstancesRemaining

  /** Runtime state of the upgrade workflow */
  4: required UpgradeWorkflowState workflowState
}


/**
 *  Raised when an operation is performed on an upgrade workflow
 *  with invalid state
 */
exception InvalidWorkflowState
{
  /** Entity ID of the workflow */
  1: required WorkflowID wfId

  /** Invalid state of the workflow */
  2: required UpgradeWorkflowState invalidState
}


/**
 *  Upgrade Workflow Manager interface
 */
service UpgradeWorkflowManager
{
  /** Create a new rolling upgrade workflow for a job */
  WorkflowID create(1: UpgradeWorkflowSpec spec) throws (
    1: common.EntityNotFound notFound,
    2: common.EntityAlreadyExists alreadyExists
  )

  /** Pause a running upgrade workflow */
  void pause(1: WorkflowID wfId) throws(
    1: common.EntityNotFound notFound,
    2: InvalidWorkflowState invalidState,
  )

  /** Resume a paused upgrade workflow */
  void resume(1: WorkflowID wfId) throws(
    1: common.EntityNotFound notFound,
    2: InvalidWorkflowState invalidState,
  )

  /** Rollback an upgrade workflow */
  void rollback(1: WorkflowID wfId) throws(
    1: common.EntityNotFound notFound,
    2: InvalidWorkflowState invalidState,
  )

  /** Abort an upgrade workflow */
  void abort(1: WorkflowID wfId, 2: bool softAbort) throws(
    1: common.EntityNotFound notFound,
    2: InvalidWorkflowState invalidState,
  )

  /** Get the upgrade workflow of a job */
  UpgradeWorkflowInfo get(1: job.JobID jobId) throws (
    1: common.EntityNotFound notFound,
  )

  /** Get all upgrade workflows that are currently running */
  list<UpgradeWorkflowInfo> getAll()
}
