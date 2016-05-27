/**
 *  Job Manager Thrift interface
 */

namespace java com.uber.infra.peloton.job

include "common.thrift"

typedef common.EntityID JobID

/**
 *  Resource configuration for a job
 */
struct ResourceConfig
{
  /** CPU limit in number of CPU cores */
  1: required double cpusLimit

  /** Memory limit in MB */
  2: required double memLimitMb

  /** Disk limit in MB */
  3: required double diskLimitMb

  /** File descriptor limit */
  4: required i32 fdLimit
}


/**
 *  Health check configuration for a job
 */
struct HealthCheckConfig
{
  /** Whether the health check is enabled. */ 
  1: optional bool enabled

  /** Start time wait in seconds */
  2: required i32 initialIntervalSecs

  /** Interval in seconds between two health checks */
  3: required i32 intervalSecs

  /** Max number of consecutive failures before failing health check */
  4: required i32 maxConsecutiveFailures

  /** Health check command timeout in seconds */
  5: required i32 timeoutSecs

  /** Health check command to be executed */
  6: required string shellCommand
}


/**
 *  Network port configuration for a job
 */
struct PortConfig
{
  /** Name of the network port, e.g. http, tchannel */
  1: required string name

  /**
   *  Static port number if any. If unset, will be dynamically allocated
   *  by the scheduler
   */
  2: optional i32 value

  /**
   *  Environment variable name to be exported when running a job for this port.
   *  Will default to UBER_PORT_{name} if unset
   */
  3: optional string envName
}


/**
 *  Access mode for a volume mount
 */
enum AccessMode
{
  READONLY  = 0,
  READWRITE = 1,
}


/**
 *  Volume mount configuration for a job
 */
struct VolumeMount
{
  /** Absolute path of the source volume to be mounted */
  1: required string srcVolume

  /** Absolute path of the destination volume to be mounted */
  2: required string dstVolume

  /** Readonly or Readwrite mode of the mount */
  3: required AccessMode accessMode
}


/**
 *  Container kind for a job
 */
enum ContainerKind
{
  MESOS = 0,
  DOCKER = 1,
}


/**
 *  Container configuration for a job
 */
struct ContainerConfig
{
  /** The container kind */
  1: required ContainerKind kind

  /** The container image to be run */
  2: required string image

  /** List of environment variables to be exported in container */
  3: optional map<string, string> env

  /** List of volumes to be mounted from host into container */
  4: optional list<VolumeMount> mounts
} 

/**
 *  SLA configuration for a job
 */
struct SlaConfig
{
  /** Priority of a job */
  1: required i32 priority

  /** Minimum viable number of instances */
  2: optional i32 minimumInstanceCount

  /** Minimum viable instance percentage */
  3: optional double minimumInstancePercent

  /**
   *  Whether the job instances are preemptible. If so, it might
   *  be scheduled using revocable offers
   */
  4: optional bool preemptible
}


/**
 *  Job configuration
 */
struct JobConfig
{
  /** Change log entry of the job config */
  1: optional common.ChangeLog changeLog

  /** Name of the job */
  2: required string name

  /** Owning team of the job */
  3: required string owningTeam

  /** LDAP groups of the job */
  4: required list<string> ldapGroups
   
  /** Description of the job */
  5: required string description

  /** List of user-defined labels for the job */
  6: optional list<common.Label> labels

  /** Number of instances of the job */
  7: required i32 instanceCount

  /** Resource config of the job */
  8: required ResourceConfig resource

  /** Health check config of the job */
  9: optional HealthCheckConfig healthCheck

  /** List of network ports to be allocated for the job */
  10: optional list<PortConfig> ports

  /** Container config of the job */
  11: required ContainerConfig container

  /** SLA config of the job */
  12: required SlaConfig sla
}


/**
 *  Job Manager service interface
 */
service JobManager
{
  /** Create a Job entity for a given config */
  JobID create(1: JobConfig config) throws (
    1: common.EntityAlreadyExists alreadyExists,
  )

  /** Get the config of a job entity */
  JobConfig get(1: JobID jobId) throws (
    1: common.EntityNotFound notFound,
  )

  /**
   *  Query the jobs that match a list of labels.
   */
  map<JobID, JobConfig> query(1: list<common.Label> labels) throws (
    1: common.EntityNotFound notFound,
  )

  /** Destroy a job entity */
  void destroy(1: JobID jobId) throws(
    1: common.EntityNotFound notFound,
  )
}
