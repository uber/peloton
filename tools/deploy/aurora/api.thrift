/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace java org.apache.aurora.gen
namespace py aurora.api

// Thrift interface definition for the aurora scheduler.

/*
 * TODO(wfarner): It would be nice if we could put some HTML tags here, regex doesn't handle it though.
 * The result of an API operation.  A result may only be specified when this is OK.
 */
enum ResponseCode {
  INVALID_REQUEST = 0,
  OK              = 1,
  ERROR           = 2,
  WARNING         = 3,
  AUTH_FAILED     = 4,
  /** Raised when a Lock-protected operation failed due to lock validation. */
  LOCK_ERROR      = 5,
  /** Raised when a scheduler is transiently unavailable and later retry is recommended. */
  ERROR_TRANSIENT = 6
}

// Aurora executor framework name.
const string AURORA_EXECUTOR_NAME = 'AuroraExecutor'

// TODO(maxim): Remove in 0.7.0. (AURORA-749)
struct Identity {
  2: string user
}

/** A single host attribute. */
struct Attribute {
  1: string name
  2: set<string> values
}

enum MaintenanceMode {
  NONE      = 1,
  SCHEDULED = 2,
  DRAINING  = 3,
  DRAINED   = 4
}

/** The attributes assigned to a host. */
struct HostAttributes {
  1: string          host
  2: set<Attribute>  attributes
  3: optional MaintenanceMode mode
  4: optional string slaveId
}

/**
 * A constraint that specifies an explicit set of values, at least one of which must be present
 * on a host for a task to be scheduled there.
 */
struct ValueConstraint {
  /** If true, treat this as a 'not' - to avoid specific values. */
  1: bool negated
  2: set<string> values
}

/**
 * A constraint the specifies the maximum number of active tasks on a host with a matching
 * attribute that may be scheduled simultaneously.
 */
struct LimitConstraint {
  1: i32 limit
}

/** Types of constraints that may be applied to a task. */
union TaskConstraint {
  1: ValueConstraint value
  2: LimitConstraint limit
}

/** A constraint that defines whether a task may be scheduled on a host. */
struct Constraint {
  /** Mesos slave attribute that the constraint is matched against. */
  1: string name
  2: TaskConstraint constraint
}

struct Package {
  1: string role
  2: string name
  3: i32 version
}

/** Arbitrary key-value metadata to be included into TaskConfig. */
struct Metadata {
  1: string key
  2: string value
}

/** A unique identifier for a Job. */
struct JobKey {
  /** User role (Unix service account), for example "mesos" */
  1: string role
  /** Environment, for example "devel" */
  2: string environment
  /** Name, for example "labrat" */
  3: string name
}

/** A unique lock key. */
union LockKey {
  1: JobKey job
}

/** A generic lock struct to facilitate context specific resource/operation serialization. */
struct Lock {
  /** ID of the lock - unique per storage */
  1: LockKey key
  /** UUID - facilitating soft lock authorization */
  2: string token
  /** Lock creator */
  3: string user
  /** Lock creation timestamp in milliseconds */
  4: i64 timestampMs
  /** Optional message to record with the lock */
  5: optional string message
}

/** A unique identifier for the active task within a job. */
struct InstanceKey {
  /** Key identifying the job. */
  1: JobKey jobKey
  /** Unique instance ID for the active task in a job. */
  2: i32 instanceId
}

/** URI which mirrors CommandInfo.URI in the Mesos Protobuf */
struct MesosFetcherURI {
  /** Where to get the resource from */
  1: string value
  /** Extract compressed archive after downloading */
  2: optional bool extract
  /** Cache value using Mesos Fetcher caching mechanism **/
  3: optional bool cache
}

struct ExecutorConfig {
  /** Name identifying the Executor. */
  1: string name
  /** Executor configuration data. */
  2: string data
}

/** The mode for a volume mount */
enum Mode {
  /** Read Write */
  RW = 1
  /** Read Only */
  RO = 2
}

/** A volume mount point within a container */
struct Volume {
  /** The path inside the container where the mount will be created. */
  1: string containerPath
  /** The path on the host that will serve as the source for the mount. */
  2: string hostPath
  /** The access mode */
  3: Mode mode
}

/** Describes an image for use with the Mesos unified containerizer in the Docker format */
struct DockerImage {
  /** The name of the image to run */
  1: string name
  /** The Docker tag identifying the image */
  2: string tag
}

/** Describes an image for use with the Mesos unified containerizer in the AppC format */
struct AppcImage {
  /** The name of the image to run */
  1: string name
  /** The appc image id identifying the image */
  2: string imageId
}

/** Describes an image to be used with the Mesos unified containerizer */
union Image {
  1: DockerImage docker
  2: AppcImage appc
}

/** Describes a mesos container, this is the default */
struct MesosContainer {
  /** the optional filesystem image to use when launching this task. */
  1: optional Image image
  /** the optional list of volumes to mount into the task. */
  2: optional list<Volume> volumes
}

/** Describes a parameter passed to docker cli */
struct DockerParameter {
  /** a parameter to pass to docker. (e.g. volume) */
  1: string name
  /** the value to pass to a parameter (e.g. /src/webapp:/opt/webapp) */
  2: string value
}

/** Describes a docker container */
struct DockerContainer {
  /** The container image to be run */
  1: string image
  /** The arbitrary parameters to pass to container */
  2: optional list<DockerParameter> parameters
}

/** Describes a container to be used in a task */
union Container {
  1: MesosContainer mesos
  2: DockerContainer docker
}

/** Describes resource value required to run a task. */
union Resource {
  1: double numCpus
  2: i64 ramMb
  3: i64 diskMb
  4: string namedPort
  5: i64 numGpus
}

/** Description of the tasks contained within a job. */
struct TaskConfig {
 /** Job task belongs to. */
 28: JobKey job
 // TODO(maxim): Deprecated. See AURORA-749.
 /** contains the role component of JobKey */
 17: Identity owner
  7: bool isService
  // TODO(maxim): Deprecated. See AURORA-1707.
  8: double numCpus
  // TODO(maxim): Deprecated. See AURORA-1707.
  9: i64 ramMb
  // TODO(maxim): Deprecated. See AURORA-1707.
 10: i64 diskMb
 11: i32 priority
 13: i32 maxTaskFailures
 // TODO(mnurolahzade): Deprecated. See AURORA-1708.
 /** Whether this is a production task, which can preempt. */
 18: optional bool production
 /** Task tier type. */
 30: optional string tier
 /** All resources required to run a task. */
 32: set<Resource> resources

 20: set<Constraint> constraints
 /** a list of named ports this task requests */
 21: set<string> requestedPorts
 /** Resources to retrieve with Mesos Fetcher */
 33: optional set<MesosFetcherURI> mesosFetcherUris
 /**
  * Custom links to include when displaying this task on the scheduler dashboard. Keys are anchor
  * text, values are URLs. Wildcards are supported for dynamic link crafting based on host, ports,
  * instance, etc.
  */
 22: optional map<string, string> taskLinks
 23: optional string contactEmail
 /** Executor configuration */
 25: optional ExecutorConfig executorConfig
 /** Used to display additional details in the UI. */
 27: optional set<Metadata> metadata

 // This field is deliberately placed at the end to work around a bug in the immutable wrapper
 // code generator.  See AURORA-1185 for details.
 /** the container the task should use to execute */
 29: Container container = { "mesos": {} }
}

struct ResourceAggregate {
  // TODO(maxim): Deprecated. See AURORA-1707.
  /** Number of CPU cores allotted. */
  1: double numCpus
  // TODO(maxim): Deprecated. See AURORA-1707.
  /** Megabytes of RAM allotted. */
  2: i64 ramMb
  // TODO(maxim): Deprecated. See AURORA-1707.
  /** Megabytes of disk space allotted. */
  3: i64 diskMb
  /** Aggregated resource values. */
  4: set<Resource> resources
}

/** Defines the policy for launching a new cron job when one is already running. */
enum CronCollisionPolicy {
  /** Kills the existing job with the colliding name, and runs the new cron job. */
  KILL_EXISTING = 0,
  /** Cancels execution of the new job, leaving the running job in tact. */
  CANCEL_NEW    = 1,
  /**
   * DEPRECATED. For existing jobs, treated the same as CANCEL_NEW.
   * createJob will reject jobs with this policy.
   */
  RUN_OVERLAP   = 2
}

/**
 * Description of an Aurora job. One task will be scheduled for each instance within the job.
 */
struct JobConfiguration {
  /**
   * Key for this job. If not specified name, owner.role, and a reasonable default environment are
   * used to construct it server-side.
   */
  9: JobKey key
  // TODO(maxim): Deprecated. See AURORA-749.
  /** Owner of this job. */
  7: Identity owner
  /**
   * If present, the job will be handled as a cron job with this crontab-syntax schedule.
   */
  4: optional string cronSchedule
  /** Collision policy to use when handling overlapping cron runs.  Default is KILL_EXISTING. */
  5: CronCollisionPolicy cronCollisionPolicy
  /** Task configuration for this job. */
  6: TaskConfig taskConfig
  /**
   * The number of instances in the job. Generated instance IDs for tasks will be in the range
   * [0, instances).
   */
  8: i32 instanceCount
}

struct JobStats {
  /** Number of tasks in active state for this job. */
  1: i32 activeTaskCount
  /** Number of tasks in finished state for this job. */
  2: i32 finishedTaskCount
  /** Number of failed tasks for this job. */
  3: i32 failedTaskCount
  /** Number of tasks in pending state for this job. */
  4: i32 pendingTaskCount
}

struct JobSummary {
  1: JobConfiguration job
  2: JobStats stats
  /** Timestamp of next cron run in ms since epoch, for a cron job */
  3: optional i64 nextCronRunMs
}

/** Closed range of integers. */
struct Range {
  1: i32 first
  2: i32 last
}

struct ConfigGroup {
  1: TaskConfig config
  3: set<Range> instances
}

struct ConfigSummary {
  1: JobKey key
  2: set<ConfigGroup> groups
}

struct PopulateJobResult {
  2: TaskConfig taskConfig
}

struct GetQuotaResult {
  /** Total allocated resource quota. */
  1: ResourceAggregate quota
  /** Resources consumed by production jobs from a shared resource pool. */
  2: optional ResourceAggregate prodSharedConsumption
  /** Resources consumed by non-production jobs from a shared resource pool. */
  3: optional ResourceAggregate nonProdSharedConsumption
  /** Resources consumed by production jobs from a dedicated resource pool. */
  4: optional ResourceAggregate prodDedicatedConsumption
  /** Resources consumed by non-production jobs from a dedicated resource pool. */
  5: optional ResourceAggregate nonProdDedicatedConsumption
}

/** States that a task may be in. */
enum ScheduleStatus {
  // TODO(maxim): This state does not add much value. Consider dropping it completely.
  /* Initial state for a task.  A task will remain in this state until it has been persisted. */
  INIT             = 11,
  /** The task will be rescheduled, but is being throttled for restarting too frequently. */
  THROTTLED        = 16,
  /** Task is awaiting assignment to a slave. */
  PENDING          = 0,
  /** Task has been assigned to a slave. */
  ASSIGNED         = 9,
  /** Slave has acknowledged receipt of task and is bootstrapping the task. */
  STARTING         = 1,
  /** The task is running on the slave. */
  RUNNING          = 2,
  /** The task terminated with an exit code of zero. */
  FINISHED         = 3,
  /** The task is being preempted by another task. */
  PREEMPTING       = 13,
  /** The task is being restarted in response to a user request. */
  RESTARTING       = 12,
  /** The task is being restarted in response to a host maintenance request. */
  DRAINING         = 17,
  /** The task terminated with a non-zero exit code. */
  FAILED           = 4,
  /** Execution of the task was terminated by the system. */
  KILLED           = 5,
  /** The task is being forcibly killed. */
  KILLING          = 6,
  /** A fault in the task environment has caused the system to believe the task no longer exists.
   * This can happen, for example, when a slave process disappears.
   */
  LOST             = 7
}

// States that a task may be in while still considered active.
const set<ScheduleStatus> ACTIVE_STATES = [ScheduleStatus.ASSIGNED,
                                           ScheduleStatus.DRAINING,
                                           ScheduleStatus.KILLING,
                                           ScheduleStatus.PENDING,
                                           ScheduleStatus.PREEMPTING,
                                           ScheduleStatus.RESTARTING
                                           ScheduleStatus.RUNNING,
                                           ScheduleStatus.STARTING,
                                           ScheduleStatus.THROTTLED]

// States that a task may be in while associated with a slave machine and non-terminal.
const set<ScheduleStatus> SLAVE_ASSIGNED_STATES = [ScheduleStatus.ASSIGNED,
                                                   ScheduleStatus.DRAINING,
                                                   ScheduleStatus.KILLING,
                                                   ScheduleStatus.PREEMPTING,
                                                   ScheduleStatus.RESTARTING,
                                                   ScheduleStatus.RUNNING,
                                                   ScheduleStatus.STARTING]

// States that a task may be in while in an active sandbox.
const set<ScheduleStatus> LIVE_STATES = [ScheduleStatus.KILLING,
                                         ScheduleStatus.PREEMPTING,
                                         ScheduleStatus.RESTARTING,
                                         ScheduleStatus.DRAINING,
                                         ScheduleStatus.RUNNING]

// States a completed task may be in.
const set<ScheduleStatus> TERMINAL_STATES = [ScheduleStatus.FAILED,
                                             ScheduleStatus.FINISHED,
                                             ScheduleStatus.KILLED,
                                             ScheduleStatus.LOST]

// Regular expressions for matching valid identifiers for job path components. All expressions
// below should accept and reject the same set of inputs.
const string GOOD_IDENTIFIER_PATTERN = "^[\\w\\-\\.]+$"
// JVM: Use with java.util.regex.Pattern#compile
const string GOOD_IDENTIFIER_PATTERN_JVM = GOOD_IDENTIFIER_PATTERN
// Python: Use with re.compile
const string GOOD_IDENTIFIER_PATTERN_PYTHON = GOOD_IDENTIFIER_PATTERN

/** Event marking a state transition within a task's lifecycle. */
struct TaskEvent {
  /** Epoch timestamp in milliseconds. */
  1: i64 timestamp
  /** New status of the task. */
  2: ScheduleStatus status
  /** Audit message that explains why a transition occurred. */
  3: optional string message
  /** Hostname of the scheduler machine that performed the event. */
  4: optional string scheduler
}

/** A task assignment that is provided to an executor. */
struct AssignedTask {
  /** The mesos task ID for this task.  Guaranteed to be globally unique */
  1: string taskId

  /**
   * The mesos slave ID that this task has been assigned to.
   * This will not be populated for a PENDING task.
   */
  2: string slaveId

  /**
   * The name of the machine that this task has been assigned to.
   * This will not be populated for a PENDING task.
   */
  3: string slaveHost

  /** Information about how to run this task. */
  4: TaskConfig task
  /** Ports reserved on the machine while this task is running. */
  5: map<string, i32> assignedPorts

  /**
   * The instance ID assigned to this task. Instance IDs must be unique and contiguous within a
   * job, and will be in the range [0, N-1] (inclusive) for a job that has N instances.
   */
  6: i32 instanceId
}

/** A task that has been scheduled. */
struct ScheduledTask {
  /** The task that was scheduled. */
  1: AssignedTask assignedTask
  /** The current status of this task. */
  2: ScheduleStatus status
  /**
   * The number of failures that this task has accumulated over the multi-generational history of
   * this task.
   */
  3: i32 failureCount
  /** State change history for this task. */
  4: list<TaskEvent> taskEvents
  /**
   * The task ID of the previous generation of this task.  When a task is automatically rescheduled,
   * a copy of the task is created and ancestor ID of the previous task's task ID.
   */
  5: string ancestorId
}

struct ScheduleStatusResult {
  1: list<ScheduledTask> tasks
}

struct GetJobsResult {
  1: set<JobConfiguration> configs
}

/**
 * Contains a set of restrictions on matching tasks where all restrictions must be met
 * (terms are AND'ed together).
 */
struct TaskQuery {
  14: string role
  9: string environment
  2: string jobName
  4: set<string> taskIds
  5: set<ScheduleStatus> statuses
  7: set<i32> instanceIds
  10: set<string> slaveHosts
  11: set<JobKey> jobKeys
  12: i32 offset
  13: i32 limit
}

struct HostStatus {
  1: string host
  2: MaintenanceMode mode
}

struct RoleSummary {
  1: string role
  2: i32 jobCount
  3: i32 cronJobCount
}

struct Hosts {
  1: set<string> hostNames
}

struct PendingReason {
  1: string taskId
  2: string reason
}

/** States that a job update may be in. */
enum JobUpdateStatus {
  /** Update is in progress. */
  ROLLING_FORWARD = 0,

  /** Update has failed and is being rolled back. */
  ROLLING_BACK = 1,

  /** Update has been paused while in progress. */
  ROLL_FORWARD_PAUSED = 2,

  /** Update has been paused during rollback. */
  ROLL_BACK_PAUSED = 3,

  /** Update has completed successfully. */
  ROLLED_FORWARD = 4,

  /** Update has failed and rolled back. */
  ROLLED_BACK = 5,

  /** Update was aborted. */
  ABORTED = 6,

  /** Unknown error during update. */
  ERROR = 7,

  /**
   * Update failed to complete.
   * This can happen if failure thresholds are met while rolling forward, but rollback is disabled,
   * or if failure thresholds are met when rolling back.
   */
  FAILED = 8,

  /** Update has been blocked while in progress due to missing/expired pulse. */
  ROLL_FORWARD_AWAITING_PULSE = 9,

  /** Update has been blocked during rollback due to missing/expired pulse. */
  ROLL_BACK_AWAITING_PULSE = 10
}

/** States the job update can be in while still considered active. */
const set<JobUpdateStatus> ACTIVE_JOB_UPDATE_STATES = [JobUpdateStatus.ROLLING_FORWARD,
                                                       JobUpdateStatus.ROLLING_BACK,
                                                       JobUpdateStatus.ROLL_FORWARD_PAUSED,
                                                       JobUpdateStatus.ROLL_BACK_PAUSED,
                                                       JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE,
                                                       JobUpdateStatus.ROLL_BACK_AWAITING_PULSE]
/** States the job update can be in while waiting for a pulse. */
const set<JobUpdateStatus> AWAITNG_PULSE_JOB_UPDATE_STATES = [JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE,
                                                              JobUpdateStatus.ROLL_BACK_AWAITING_PULSE]

/** Job update actions that can be applied to job instances. */
enum JobUpdateAction {
  /**
   * An instance was moved to the target state successfully, and declared healthy if the desired
   * state did not involve deleting the instance.
   */
  INSTANCE_UPDATED = 1,

  /**
   * An instance was rolled back because the job update did not succeed.  The instance was reverted
   * to the original state prior to the job update, which means that the instance was removed if
   * the update added instances to the job.
   */
  INSTANCE_ROLLED_BACK = 2,

  /**
   * An instance is being moved from the original state to the desired state.
   */
  INSTANCE_UPDATING = 3,

  /**
   * An instance is being moved from the desired state back to the original state, because the job
   * update failed.
   */
  INSTANCE_ROLLING_BACK = 4,

  /** An instance update was attempted but failed and was not rolled back. */
  INSTANCE_UPDATE_FAILED = 5,

  /** An instance rollback was attempted but failed. */
  INSTANCE_ROLLBACK_FAILED = 6
}

/** Status of the coordinated update. Intended as a response to pulseJobUpdate RPC. */
enum JobUpdatePulseStatus {
  /**
   *  Update is active. See ACTIVE_JOB_UPDATE_STATES for statuses considered active.
   */
  OK = 1,

  /**
   * Update has reached terminal state. See TERMINAL_JOB_UPDATE_STATES for statuses
   * considered terminal.
   */
  FINISHED = 2
}

/** Job update key. */
struct JobUpdateKey {
  /** Job being updated */
  1: JobKey job

  /** Update ID. */
  2: string id
}

/** Job update thresholds and limits. */
struct JobUpdateSettings {
  /** Max number of instances being updated at any given moment. */
  1: i32 updateGroupSize

  /** Max number of instance failures to tolerate before marking instance as FAILED. */
  2: i32 maxPerInstanceFailures

  /** Max number of FAILED instances to tolerate before terminating the update. */
  3: i32 maxFailedInstances

  /** Min time to watch a RUNNING instance. */
  5: i32 minWaitInInstanceRunningMs

  /** If true, enables failed update rollback. */
  6: bool rollbackOnFailure

  /** Instance IDs to act on. All instances will be affected if this is not set. */
  7: set<Range> updateOnlyTheseInstances

  /**
   * If true, use updateGroupSize as strict batching boundaries, and avoid proceeding to another
   * batch until the preceding batch finishes updating.
   */
  8: bool waitForBatchCompletion

 /**
  * If set, requires external calls to pulseJobUpdate RPC within the specified rate for the
  * update to make progress. If no pulses received within specified interval the update will
  * block. A blocked update is unable to continue but retains its current status. It may only get
  * unblocked by a fresh pulseJobUpdate call.
  */
  9: optional i32 blockIfNoPulsesAfterMs
}

/** Event marking a state transition in job update lifecycle. */
struct JobUpdateEvent {
  /** Update status. */
  1: JobUpdateStatus status

  /** Epoch timestamp in milliseconds. */
  2: i64 timestampMs

  /** User who performed this event (if user-initiated). */
  3: optional string user

  /**
   * Message from the user (for user-initiated transitions) or the scheduler about why the state was
   * changed.
   */
  4: optional string message
}

/** Event marking a state transition in job instance update lifecycle. */
struct JobInstanceUpdateEvent {
  /** Job instance ID. */
  1: i32 instanceId

  /** Epoch timestamp in milliseconds. */
  2: i64 timestampMs

  /** Job update action taken on the instance. */
  3: JobUpdateAction action
}

/** Maps instance IDs to TaskConfigs it. */
struct InstanceTaskConfig {
  /** A TaskConfig associated with instances. */
  1: TaskConfig task

  /** Instances associated with the TaskConfig. */
  2: set<Range> instances
}

/** Current job update state including status and created/modified timestamps. */
struct JobUpdateState {
  /** Current status of the update. */
  1: JobUpdateStatus status

  /** Created timestamp in milliseconds. */
  2: i64 createdTimestampMs

  /** Last modified timestamp in milliseconds. */
  3: i64 lastModifiedTimestampMs
}

/** Summary of the job update including job key, user and current state. */
struct JobUpdateSummary {
  /** Unique identifier for the update. */
  5: JobUpdateKey key

  /** User initiated an update. */
  3: string user

  /** Current job update state. */
  4: JobUpdateState state

  /** Update metadata supplied by the client. */
  6: optional set<Metadata> metadata
}

/** Update configuration and setting details. */
struct JobUpdateInstructions {
  /** Actual InstanceId -> TaskConfig mapping when the update was requested. */
  1: set<InstanceTaskConfig> initialState

  /** Desired configuration when the update completes. */
  2: InstanceTaskConfig desiredState

  /** Update specific settings. */
  3: JobUpdateSettings settings
}

/** Full definition of the job update. */
struct JobUpdate {
  /** Update summary. */
  1: JobUpdateSummary summary

  /** Update configuration. */
  2: JobUpdateInstructions instructions
}

struct JobUpdateDetails {
  /** Update definition. */
  1: JobUpdate update

  /** History for this update. */
  2: list<JobUpdateEvent> updateEvents

  /** History for the individual instances updated. */
  3: list<JobInstanceUpdateEvent> instanceEvents
}

/** A request to update the following instances of an existing job. Used by startUpdate. */
struct JobUpdateRequest {
  /** Desired TaskConfig to apply. */
  1: TaskConfig taskConfig

  /** Desired number of instances of the task config. */
  2: i32 instanceCount

  /** Update settings and limits. */
  3: JobUpdateSettings settings

  /** Update metadata supplied by the client issuing the JobUpdateRequest. */
  4: optional set<Metadata> metadata
}

/**
 * Contains a set of restrictions on matching job updates where all restrictions must be met
 * (terms are AND'ed together).
 */
struct JobUpdateQuery {
  /** Job role. */
  2: string role

  /** Unique identifier for a job update. */
  8: JobUpdateKey key

  /** Job key. */
  3: JobKey jobKey

  /** User who created the update. */
  4: string user

  /** Set of update statuses. */
  5: set<JobUpdateStatus> updateStatuses

  /** Offset to serve data from. Used by pagination. */
  6: i32 offset

  /** Number or records to serve. Used by pagination. */
  7: i32 limit
}

struct ListBackupsResult {
  1: set<string> backups
}

struct StartMaintenanceResult {
  1: set<HostStatus> statuses
}

struct DrainHostsResult {
  1: set<HostStatus> statuses
}

struct QueryRecoveryResult {
  1: set<ScheduledTask> tasks
}

struct MaintenanceStatusResult {
  1: set<HostStatus> statuses
}

struct EndMaintenanceResult {
  1: set<HostStatus> statuses
}

struct RoleSummaryResult {
  1: set<RoleSummary> summaries
}

struct JobSummaryResult {
  1: set<JobSummary> summaries
}

struct ConfigSummaryResult {
  1: ConfigSummary summary
}

struct GetPendingReasonResult {
  1: set<PendingReason> reasons
}

/** Result of the startUpdate call. */
struct StartJobUpdateResult {
  /** Unique identifier for the job update. */
  1: JobUpdateKey key

  /** Summary of the update that is in progress for the given JobKey. */
  2: optional JobUpdateSummary updateSummary
}

/** Result of the getJobUpdateSummaries call. */
struct GetJobUpdateSummariesResult {
  1: list<JobUpdateSummary> updateSummaries
}

/** Result of the getJobUpdateDetails call. */
struct GetJobUpdateDetailsResult {
  // TODO(zmanji): Remove this once we complete AURORA-1765
  1: JobUpdateDetails details
  2: list<JobUpdateDetails> detailsList
}

/** Result of the pulseJobUpdate call. */
struct PulseJobUpdateResult {
  1: JobUpdatePulseStatus status
}

struct GetJobUpdateDiffResult {
  /** Instance addition diff details. */
  1: set<ConfigGroup> add

  /** Instance removal diff details. */
  2: set<ConfigGroup> remove

  /** Instance update diff details. */
  3: set<ConfigGroup> update

  /** Instances unchanged by the update. */
  4: set<ConfigGroup> unchanged
}

/** Tier information. */
struct TierConfig {
  /** Name of tier. */
  1: string name
  /** Tier attributes. */
  2: map<string, string> settings
}

/** Result of the getTierConfigResult call. */
struct GetTierConfigResult {
  /** Name of the default tier. */
  1: string defaultTierName
  /** Set of tier configurations. */
  2: set<TierConfig> tiers
}

/** Information about the scheduler. */
struct ServerInfo {
  1: string clusterName
  /** A url prefix for job container stats. */
  3: string statsUrlPrefix
}

union Result {
  1: PopulateJobResult populateJobResult
  3: ScheduleStatusResult scheduleStatusResult
  4: GetJobsResult getJobsResult
  5: GetQuotaResult getQuotaResult
  6: ListBackupsResult listBackupsResult
  7: StartMaintenanceResult startMaintenanceResult
  8: DrainHostsResult drainHostsResult
  9: QueryRecoveryResult queryRecoveryResult
  10: MaintenanceStatusResult maintenanceStatusResult
  11: EndMaintenanceResult endMaintenanceResult
  17: RoleSummaryResult roleSummaryResult
  18: JobSummaryResult jobSummaryResult
  20: ConfigSummaryResult configSummaryResult
  21: GetPendingReasonResult getPendingReasonResult
  22: StartJobUpdateResult startJobUpdateResult
  23: GetJobUpdateSummariesResult getJobUpdateSummariesResult
  24: GetJobUpdateDetailsResult getJobUpdateDetailsResult
  25: PulseJobUpdateResult pulseJobUpdateResult
  26: GetJobUpdateDiffResult getJobUpdateDiffResult
  27: GetTierConfigResult getTierConfigResult
}

struct ResponseDetail {
  1: string message
}

struct Response {
  1: ResponseCode responseCode
  5: ServerInfo serverInfo
  /** Payload from the invoked RPC. */
  3: optional Result result
  /**
   * Messages from the server relevant to the request, such as warnings or use of deprecated
   * features.
   */
  6: list<ResponseDetail> details
}

// A service that provides all the read only calls to the Aurora scheduler.
service ReadOnlyScheduler {
  /** Returns a summary of the jobs grouped by role. */
  Response getRoleSummary()

  /** Returns a summary of jobs, optionally only those owned by a specific role. */
  Response getJobSummary(1: string role)

  /** Fetches the status of tasks. */
  Response getTasksStatus(1: TaskQuery query)

  /**
   * Same as getTaskStatus but without the TaskConfig.ExecutorConfig data set.
   * This is an interim solution until we have a better way to query TaskConfigs (AURORA-541).
   */
  Response getTasksWithoutConfigs(1: TaskQuery query)

  /** Returns user-friendly reasons (if available) for tasks retained in PENDING state. */
  Response getPendingReason(1: TaskQuery query)

  /** Fetches the configuration summary of active tasks for the specified job. */
  Response getConfigSummary(1: JobKey job)

  /**
   * Fetches the status of jobs.
   * ownerRole is optional, in which case all jobs are returned.
   */
  Response getJobs(1: string ownerRole)

  /** Fetches the quota allocated for a user. */
  Response getQuota(1: string ownerRole)

  /**
   * Populates fields in a job configuration as though it were about to be run.
   * This can be used to diff a configuration running tasks.
   */
  Response populateJobConfig(1: JobConfiguration description)

  /** Gets job update summaries. */
  Response getJobUpdateSummaries(1: JobUpdateQuery jobUpdateQuery)

  /** Gets job update details. */
  // TODO(zmanji): `key` is deprecated, remove this with AURORA-1765
  Response getJobUpdateDetails(1: JobUpdateKey key, 2: JobUpdateQuery query)

  /** Gets the diff between client (desired) and server (current) job states. */
  Response getJobUpdateDiff(1: JobUpdateRequest request)

  /** Gets tier configurations. */
  Response getTierConfigs()
}

service AuroraSchedulerManager extends ReadOnlyScheduler {
  /**
   * Creates a new job.  The request will be denied if a job with the provided name already exists
   * in the cluster.
   */
  Response createJob(1: JobConfiguration description)

  /**
   * Enters a job into the cron schedule, without actually starting the job.
   * If the job is already present in the schedule, this will update the schedule entry with the new
   * configuration.
   */
  Response scheduleCronJob(1: JobConfiguration description)

  /**
   * Removes a job from the cron schedule. The request will be denied if the job was not previously
   * scheduled with scheduleCronJob.
   */
  Response descheduleCronJob(4: JobKey job)

  /**
   * Starts a cron job immediately.  The request will be denied if the specified job does not
   * exist for the role account, or the job is not a cron job.
   */
  Response startCronJob(4: JobKey job)

  /** Restarts a batch of shards. */
  Response restartShards(5: JobKey job, 3: set<i32> shardIds)

  /** Initiates a kill on tasks. */
  Response killTasks(4: JobKey job, 5: set<i32> instances, 6: string message)

  /**
   * Adds new instances with the TaskConfig of the existing instance pointed by the key.
   */
  Response addInstances(3: InstanceKey key, 4: i32 count)

  // TODO(maxim): reevaluate if it's still needed when client updater is gone (AURORA-785).
  /**
   * Replaces the template (configuration) for the existing cron job.
   * The cron job template (configuration) must exist for the call to succeed.
   */
  Response replaceCronTemplate(1: JobConfiguration config)

  /** Starts update of the existing service job. */
  Response startJobUpdate(
      /** A description of how to change the job. */
      1: JobUpdateRequest request,
      /** A user-specified message to include with the induced job update state change. */
      3: string message)

  /**
   * Pauses the specified job update. Can be resumed by resumeUpdate call.
   */
  Response pauseJobUpdate(
      /** The update to pause. */
      1: JobUpdateKey key,
      /** A user-specified message to include with the induced job update state change. */
      3: string message)

  /** Resumes progress of a previously paused job update. */
  Response resumeJobUpdate(
      /** The update to resume. */
      1: JobUpdateKey key,
      /** A user-specified message to include with the induced job update state change. */
      3: string message)

  /** Permanently aborts the job update. Does not remove the update history. */
  Response abortJobUpdate(
      /** The update to abort. */
      1: JobUpdateKey key,
      /** A user-specified message to include with the induced job update state change. */
      3: string message)

  /**
   * Rollbacks the specified active job update to the initial state.
   */
  Response rollbackJobUpdate(
      /** The update to rollback. */
      1: JobUpdateKey key,
      /** A user-specified message to include with the induced job update state change. */
      2: string message)

  /**
   * Allows progress of the job update in case blockIfNoPulsesAfterMs is specified in
   * JobUpdateSettings. Unblocks progress if the update was previously blocked.
   * Responds with ResponseCode.INVALID_REQUEST in case an unknown update key is specified.
   */
  Response pulseJobUpdate(1: JobUpdateKey key)
}

struct InstanceConfigRewrite {
  /** Key for the task to rewrite. */
  1: InstanceKey instanceKey
  /** The original configuration. */
  2: TaskConfig oldTask
  /** The rewritten configuration. */
  3: TaskConfig rewrittenTask
}

struct JobConfigRewrite {
  /** The original job configuration. */
  1: JobConfiguration oldJob
  /** The rewritten job configuration. */
  2: JobConfiguration rewrittenJob
}

union ConfigRewrite {
  1: JobConfigRewrite jobRewrite
  2: InstanceConfigRewrite instanceRewrite
}

struct RewriteConfigsRequest {
  1: list<ConfigRewrite> rewriteCommands
}

struct ExplicitReconciliationSettings {
  1: optional i32 batchSize
}

// It would be great to compose these services rather than extend, but that won't be possible until
// https://issues.apache.org/jira/browse/THRIFT-66 is resolved.
service AuroraAdmin extends AuroraSchedulerManager {
  /** Assign quota to a user.  This will overwrite any pre-existing quota for the user. */
  Response setQuota(1: string ownerRole, 2: ResourceAggregate quota)

  /**
   * Forces a task into a specific state.  This does not guarantee the task will enter the given
   * state, as the task must still transition within the bounds of the state machine.  However,
   * it attempts to enter that state via the state machine.
   */
  Response forceTaskState(
      1: string taskId,
      2: ScheduleStatus status)

  /** Immediately writes a storage snapshot to disk. */
  Response performBackup()

  /** Lists backups that are available for recovery. */
  Response listBackups()

  /** Loads a backup to an in-memory storage.  This must precede all other recovery operations. */
  Response stageRecovery(1: string backupId)

  /** Queries for tasks in a staged recovery. */
  Response queryRecovery(1: TaskQuery query)

  /** Deletes tasks from a staged recovery. */
  Response deleteRecoveryTasks(1: TaskQuery query)

  /** Commits a staged recovery, completely replacing the previous storage state. */
  Response commitRecovery()

  /** Unloads (aborts) a staged recovery. */
  Response unloadRecovery()

  /** Put the given hosts into maintenance mode. */
  Response startMaintenance(1: Hosts hosts)

  /** Ask scheduler to begin moving tasks scheduled on given hosts. */
  Response drainHosts(1: Hosts hosts)

  /** Retrieve the current maintenance states for a group of hosts. */
  Response maintenanceStatus(1: Hosts hosts)

  /** Set the given hosts back into serving mode. */
  Response endMaintenance(1: Hosts hosts)

  /** Start a storage snapshot and block until it completes. */
  Response snapshot()

  /**
   * Forcibly rewrites the stored definition of user configurations.  This is intended to be used
   * in a controlled setting, primarily to migrate pieces of configurations that are opaque to the
   * scheduler (e.g. executorConfig).
   * The scheduler may do some validation of the rewritten configurations, but it is important
   * that the caller take care to provide valid input and alter only necessary fields.
   */
  Response rewriteConfigs(1: RewriteConfigsRequest request)

  /** Tell scheduler to trigger an explicit task reconciliation with the given settings. */
  Response triggerExplicitTaskReconciliation(1: ExplicitReconciliationSettings settings)

  /** Tell scheduler to trigger an implicit task reconciliation. */
  Response triggerImplicitTaskReconciliation()

  /**
   * Force prune any (terminal) tasks that match the query. If no statuses are supplied with the
   * query, it will default to all terminal task states. If statuses are supplied, they must be
   * terminal states.
   */
  Response pruneTasks(1: TaskQuery query)
}

// The name of the header that should be sent to bypass leader redirection in the Scheduler.
const string BYPASS_LEADER_REDIRECT_HEADER_NAME = 'Bypass-Leader-Redirect'

// The path under which a task's filesystem should be mounted when using images and the Mesos
// unified containerizer.
const string TASK_FILESYSTEM_MOUNT_POINT = 'taskfs'
