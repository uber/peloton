/**
 *  Instance Manager Thrift interface
 */

namespace java com.uber.infra.peloton

include "common.thrift"
include "job.thrift"

typedef i32 InstanceID

typedef string TaskID


/**
 *  Runtime states of an instance
 */
enum RuntimeState {
  /** The instance is being initialized */
  INITIALIZED = 0

  /** The instance is being scheduled */
  SCHEDULING = 1

  /** The instance has been assigned to a node */
  ASSIGNED   = 2

  /** The instance has been launched by Mesos */
  LAUNCHED   = 3

  /** The instance is running on a node */
  RUNNING    = 4

  /** The instance terminated with an exit code of zero */
  SUCCEEDED  = 5

  /** The instance terminated with a non-zero exit code. */
  FAILED     = 6

  /** Execution of the instance was terminated by the system. */
  KILLED     = 7

  /** The instance is being preempted by another one on the node. */
  PREEMPTING = 8

  /** The instance is being throttled for restarting too frequently. */
  THROTTLED  = 9
}


/**
 *  Runtime info of an instance in a Job
 */
struct RuntimeInfo {

  /** Runtime status of the instance */
  1: required RuntimeState state

  /** The mesos task ID for this instance. */
  2: optional TaskID taskId

  /** The start time of the instance. The instance is stopped when unset. */
  3: optional common.DateTime startedAt

  /** The name of the host where the instance is running. */
  4: optional string host

  /** Ports reserved on the host while this instance is running. */
  5: optional map<string, i32> ports
}


/**
 *  Info of an instance in a Job
 */
struct InstanceInfo {

  /**
   *  The numerical ID assigned to this instance. Instance IDs must be
   *  unique and contiguous within a job. The ID is in the range of
   *  [0, N-1] for a job with instance count of N.
   */
  1: required InstanceID instanceId

  /** Job ID of the instance */
  2: required job.JobID jobId

  /** Job configuration of the instance */
  3: required job.JobConfig jobConfig  

  /** Runtime info of the instance */
  4: optional RuntimeInfo runtime
}


/** Raised when a given instance Id is out of range */
exception InstanceIdOutOfRange
{
  /** Entity ID of the job */
  1: required job.JobID jobId

  /** Instance count of the job */
  2: required i32 instanceCount
}


/**
 *  Instance manager interface
 */
service InstanceManager
{
  /**
   *  Get the instance info for a job instance.
   */
  InstanceInfo get(1: job.JobID jobId, 2: InstanceID instId) throws (
    1: common.EntityNotFound notFound,
    2: InstanceIdOutOfRange outOfRange,
  )

  /**
   *  Get the instance info of all job instances.
   */
  map<InstanceID, InstanceInfo> getAll(1: job.JobID jobId) throws (
    1: common.EntityNotFound notFound
  )
  
  /**
   *  Start a set of instances for a job. Will be no-op for instances that
   *  are currently running.
   */
  void startInstances(1: job.JobID jobId, 2: set<InstanceID> instIds) throws (
    1: common.EntityNotFound notFound,
  )

  /**
   *  Stop a set of instances for a job. Will be no-op for instances that
   *  are currently stopped.
   */
  void stopInstances(1: job.JobID jobId, 2: set<InstanceID> instIds) throws (
    1: common.EntityNotFound notFound,
  )

  /**
   *  Restart a set of instances for a job. Will start instances that are
   *  currently stopped.
   */
  void restartInstances(1: job.JobID jobId, 2: set<InstanceID> instIds) throws (
    1: common.EntityNotFound notFound,
  )
}
