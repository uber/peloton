package storage

import (
	"github.com/uber-go/tally"
)

// JobMetrics is a struct for tracking all the job related counters in the storage layer
type JobMetrics struct {
	JobCreate           tally.Counter
	JobCreateConfig     tally.Counter
	JobCreateFail       tally.Counter
	JobCreateConfigFail tally.Counter

	JobCreateRuntime     tally.Counter
	JobCreateRuntimeFail tally.Counter

	JobUpdate     tally.Counter
	JobUpdateFail tally.Counter

	JobGet      tally.Counter
	JobGetFail  tally.Counter
	JobNotFound tally.Counter

	JobQuery     tally.Counter
	JobQueryAll  tally.Counter
	JobQueryFail tally.Counter

	JobDelete     tally.Counter
	JobDeleteFail tally.Counter

	JobGetRuntime     tally.Counter
	JobGetRuntimeFail tally.Counter

	JobGetByStates     tally.Counter
	JobGetByStatesFail tally.Counter

	JobGetAll     tally.Counter
	JobGetAllFail tally.Counter

	JobGetByRespoolID     tally.Counter
	JobGetByRespoolIDFail tally.Counter

	JobUpdateRuntime     tally.Counter
	JobUpdateRuntimeFail tally.Counter

	JobUpdateConfig     tally.Counter
	JobUpdateConfigFail tally.Counter

	JobUpdateInfo     tally.Counter
	JobUpdateInfoFail tally.Counter
}

// TaskMetrics is a struct for tracking all the task related counters in the storage layer
type TaskMetrics struct {
	TaskCreate     tally.Counter
	TaskCreateFail tally.Counter

	TaskCreateConfig     tally.Counter
	TaskCreateConfigFail tally.Counter

	TaskGet      tally.Counter
	TaskGetFail  tally.Counter
	TaskNotFound tally.Counter

	TaskGetConfig     tally.Counter
	TaskGetConfigFail tally.Counter

	TaskGetConfigs     tally.Counter
	TaskGetConfigsFail tally.Counter

	TaskLogState     tally.Counter
	TaskLogStateFail tally.Counter

	TaskGetLogState     tally.Counter
	TaskGetLogStateFail tally.Counter

	TaskGetForJob     tally.Counter
	TaskGetForJobFail tally.Counter

	TaskGetForJobAndState     tally.Counter
	TaskGetForJobAndStateFail tally.Counter

	TaskSummaryForJob     tally.Counter
	TaskSummaryForJobFail tally.Counter

	TaskGetForJobRange     tally.Counter
	TaskGetForJobRangeFail tally.Counter

	TaskGetRuntimesForJobRange     tally.Counter
	TaskGetRuntimesForJobRangeFail tally.Counter

	TaskGetRuntime     tally.Counter
	TaskGetRuntimeFail tally.Counter

	TaskUpdateRuntime     tally.Counter
	TaskUpdateRuntimeFail tally.Counter

	TaskDelete     tally.Counter
	TaskDeleteFail tally.Counter

	TaskUpdate     tally.Counter
	TaskUpdateFail tally.Counter
}

// ResourcePoolMetrics is a struct for tracking resource pool related counters in the storage layer
type ResourcePoolMetrics struct {
	ResourcePoolCreate     tally.Counter
	ResourcePoolCreateFail tally.Counter

	ResourcePoolUpdate     tally.Counter
	ResourcePoolUpdateFail tally.Counter

	ResourcePoolDelete     tally.Counter
	ResourcePoolDeleteFail tally.Counter

	ResourcePoolGet     tally.Counter
	ResourcePoolGetFail tally.Counter
}

// FrameworkStoreMetrics is a struct for tracking framework and streamID related counters in the storage layer
type FrameworkStoreMetrics struct {
	FrameworkUpdate     tally.Counter
	FrameworkUpdateFail tally.Counter
	FrameworkIDGet      tally.Counter
	FrameworkIDGetFail  tally.Counter
	StreamIDGet         tally.Counter
	StreamIDGetFail     tally.Counter
}

// VolumeMetrics is a struct for tracking disk related counters in the storage layer
type VolumeMetrics struct {
	VolumeCreate     tally.Counter
	VolumeCreateFail tally.Counter
	VolumeUpdate     tally.Counter
	VolumeUpdateFail tally.Counter
	VolumeGet        tally.Counter
	VolumeGetFail    tally.Counter
	VolumeDelete     tally.Counter
	VolumeDeleteFail tally.Counter
}

// ErrorMetrics is a struct for tracking all the storage error counters
type ErrorMetrics struct {
	ReadFailure        tally.Counter
	WriteFailure       tally.Counter
	AlreadyExists      tally.Counter
	ReadTimeout        tally.Counter
	WriteTimeout       tally.Counter
	RequestUnavailable tally.Counter
	TooManyTimeouts    tally.Counter
	ConnUnavailable    tally.Counter
	SessionClosed      tally.Counter
	NoConnections      tally.Counter
	ConnectionClosed   tally.Counter
	NoStreams          tally.Counter
	NotTransient       tally.Counter
	CASNotApplied      tally.Counter
}

// Metrics is a struct for tracking all the general purpose counters that have relevance to the storage
// layer, i.e. how many jobs and tasks were created/deleted in the storage layer
type Metrics struct {
	JobMetrics            *JobMetrics
	TaskMetrics           *TaskMetrics
	ResourcePoolMetrics   *ResourcePoolMetrics
	FrameworkStoreMetrics *FrameworkStoreMetrics
	VolumeMetrics         *VolumeMetrics
	ErrorMetrics          *ErrorMetrics
}

// NewMetrics returns a new Metrics struct, with all metrics initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	jobScope := scope.SubScope("job")
	jobSuccessScope := jobScope.Tagged(map[string]string{"result": "success"})
	jobFailScope := jobScope.Tagged(map[string]string{"result": "fail"})
	jobNotFoundScope := jobScope.Tagged(map[string]string{"result": "not_found"})

	taskScope := scope.SubScope("task")
	taskSuccessScope := taskScope.Tagged(map[string]string{"result": "success"})
	taskFailScope := taskScope.Tagged(map[string]string{"result": "fail"})
	taskNotFoundScope := taskScope.Tagged(map[string]string{"result": "not_found"})

	resourcePoolScope := scope.SubScope("resource_pool")
	resourcePoolSuccessScope := resourcePoolScope.Tagged(map[string]string{"result": "success"})
	resourcePoolFailScope := resourcePoolScope.Tagged(map[string]string{"result": "fail"})

	frameworkIDScope := scope.SubScope("framework_id")
	frameworkIDSuccessScope := frameworkIDScope.Tagged(map[string]string{"result": "success"})
	frameworkIDFailScope := frameworkIDScope.Tagged(map[string]string{"result": "fail"})

	streamIDScope := scope.SubScope("stream_id")
	streamIDSuccessScope := streamIDScope.Tagged(map[string]string{"result": "success"})
	streamIDFailScope := streamIDScope.Tagged(map[string]string{"result": "fail"})

	volumeScope := scope.SubScope("persistent_volume")
	volumeSuccessScope := volumeScope.Tagged(map[string]string{"result": "success"})
	volumeFailScope := volumeScope.Tagged(map[string]string{"result": "fail"})

	storageErrorScope := scope.SubScope("storage_error")

	jobMetrics := &JobMetrics{
		JobCreate:            jobSuccessScope.Counter("create"),
		JobCreateConfig:      jobSuccessScope.Counter("create_config"),
		JobCreateFail:        jobFailScope.Counter("create"),
		JobCreateConfigFail:  jobFailScope.Counter("create_config"),
		JobCreateRuntime:     jobSuccessScope.Counter("create_runtime"),
		JobCreateRuntimeFail: jobFailScope.Counter("create_runtime"),

		JobDelete:     jobSuccessScope.Counter("delete"),
		JobDeleteFail: jobFailScope.Counter("delete"),
		JobGet:        jobSuccessScope.Counter("get"),
		JobGetFail:    jobFailScope.Counter("get"),
		JobUpdate:     jobSuccessScope.Counter("update"),
		JobUpdateFail: jobFailScope.Counter("update"),
		JobNotFound:   jobNotFoundScope.Counter("get"),

		JobQuery:     jobSuccessScope.Counter("query"),
		JobQueryAll:  jobSuccessScope.Counter("query_all"),
		JobQueryFail: jobFailScope.Counter("query"),

		JobGetRuntime:         jobSuccessScope.Counter("get_runtime"),
		JobGetRuntimeFail:     jobFailScope.Counter("get_runtime"),
		JobGetByStates:        jobSuccessScope.Counter("get_job_by_state"),
		JobGetByStatesFail:    jobFailScope.Counter("get_job_by_state"),
		JobGetAll:             jobSuccessScope.Counter("get_job_all"),
		JobGetAllFail:         jobFailScope.Counter("get_job_all"),
		JobGetByRespoolID:     jobSuccessScope.Counter("get_job_by_respool_id"),
		JobGetByRespoolIDFail: jobFailScope.Counter("get_job_by_respool_id"),
		JobUpdateRuntime:      jobSuccessScope.Counter("update_runtime"),
		JobUpdateRuntimeFail:  jobFailScope.Counter("update_runtime"),
		JobUpdateConfig:       jobSuccessScope.Counter("update_config"),
		JobUpdateConfigFail:   jobFailScope.Counter("update_config"),

		JobUpdateInfo:     jobSuccessScope.Counter("update_info"),
		JobUpdateInfoFail: jobFailScope.Counter("update_info"),
	}

	taskMetrics := &TaskMetrics{
		TaskCreate:           taskSuccessScope.Counter("create"),
		TaskCreateFail:       taskFailScope.Counter("create"),
		TaskCreateConfig:     taskSuccessScope.Counter("create_config"),
		TaskCreateConfigFail: taskFailScope.Counter("create_config"),
		TaskGet:              taskSuccessScope.Counter("get"),
		TaskGetFail:          taskFailScope.Counter("get"),
		TaskGetConfig:        taskSuccessScope.Counter("get_config"),
		TaskGetConfigFail:    taskFailScope.Counter("get_config"),
		TaskGetConfigs:       taskSuccessScope.Counter("get_configs"),
		TaskGetConfigsFail:   taskFailScope.Counter("get_configs"),

		TaskLogState:        taskSuccessScope.Counter("log_state"),
		TaskLogStateFail:    taskFailScope.Counter("log_state"),
		TaskGetLogState:     taskSuccessScope.Counter("get_log_state"),
		TaskGetLogStateFail: taskFailScope.Counter("get_log_state"),

		TaskGetForJob:                  taskSuccessScope.Counter("get_for_job"),
		TaskGetForJobFail:              taskFailScope.Counter("get_for_job"),
		TaskGetForJobAndState:          taskSuccessScope.Counter("get_for_job_and_state"),
		TaskGetForJobAndStateFail:      taskFailScope.Counter("get_for_job_and_state"),
		TaskSummaryForJob:              taskSuccessScope.Counter("summary_for_job"),
		TaskSummaryForJobFail:          taskFailScope.Counter("summary_for_job"),
		TaskGetForJobRange:             taskSuccessScope.Counter("get_for_job_range"),
		TaskGetForJobRangeFail:         taskFailScope.Counter("get_for_job_range"),
		TaskGetRuntimesForJobRange:     taskSuccessScope.Counter("get_runtimes_for_job_range"),
		TaskGetRuntimesForJobRangeFail: taskFailScope.Counter("get_runtimes_for_job_range"),

		TaskGetRuntime:        taskSuccessScope.Counter("get_runtime"),
		TaskGetRuntimeFail:    taskFailScope.Counter("get_runtime"),
		TaskUpdateRuntime:     taskSuccessScope.Counter("update_runtime"),
		TaskUpdateRuntimeFail: taskFailScope.Counter("update_runtime"),

		TaskDelete:     taskSuccessScope.Counter("delete"),
		TaskDeleteFail: taskFailScope.Counter("delete"),
		TaskUpdate:     taskSuccessScope.Counter("update"),
		TaskUpdateFail: taskFailScope.Counter("update"),
		TaskNotFound:   taskNotFoundScope.Counter("get"),
	}

	resourcePoolMetrics := &ResourcePoolMetrics{
		ResourcePoolCreate:     resourcePoolSuccessScope.Counter("create"),
		ResourcePoolCreateFail: resourcePoolFailScope.Counter("create"),
		ResourcePoolUpdate:     resourcePoolSuccessScope.Counter("update"),
		ResourcePoolUpdateFail: resourcePoolFailScope.Counter("update"),
		ResourcePoolDelete:     resourcePoolSuccessScope.Counter("delete"),
		ResourcePoolDeleteFail: resourcePoolFailScope.Counter("delete"),
		ResourcePoolGet:        resourcePoolSuccessScope.Counter("get"),
		ResourcePoolGetFail:    resourcePoolFailScope.Counter("get"),
	}

	frameworkStoreMetrics := &FrameworkStoreMetrics{
		FrameworkIDGet:      frameworkIDSuccessScope.Counter("get"),
		FrameworkIDGetFail:  frameworkIDFailScope.Counter("get"),
		FrameworkUpdate:     frameworkIDSuccessScope.Counter("update"),
		FrameworkUpdateFail: frameworkIDFailScope.Counter("update"),

		StreamIDGet:     streamIDSuccessScope.Counter("get"),
		StreamIDGetFail: streamIDFailScope.Counter("get"),
	}

	volumeMetrics := &VolumeMetrics{
		VolumeCreate:     volumeSuccessScope.Counter("create"),
		VolumeCreateFail: volumeFailScope.Counter("create"),
		VolumeGet:        volumeSuccessScope.Counter("get"),
		VolumeGetFail:    volumeFailScope.Counter("get"),
		VolumeUpdate:     volumeSuccessScope.Counter("update"),
		VolumeUpdateFail: volumeFailScope.Counter("update"),
		VolumeDelete:     volumeSuccessScope.Counter("delete"),
		VolumeDeleteFail: volumeFailScope.Counter("delete"),
	}

	errorMetrics := &ErrorMetrics{
		ReadFailure:        storageErrorScope.Counter("read_failure"),
		WriteFailure:       storageErrorScope.Counter("write_failure"),
		AlreadyExists:      storageErrorScope.Counter("already_exists"),
		ReadTimeout:        storageErrorScope.Counter("read_timeout"),
		WriteTimeout:       storageErrorScope.Counter("write_timeout"),
		RequestUnavailable: storageErrorScope.Counter("request_unavailable"),
		TooManyTimeouts:    storageErrorScope.Counter("too_many_timeouts"),
		ConnUnavailable:    storageErrorScope.Counter("conn_unavailable"),
		SessionClosed:      storageErrorScope.Counter("session_closed"),
		NoConnections:      storageErrorScope.Counter("no_connections"),
		ConnectionClosed:   storageErrorScope.Counter("connection_closed"),
		NoStreams:          storageErrorScope.Counter("no_streams"),
		NotTransient:       storageErrorScope.Counter("not_transient"),
		CASNotApplied:      storageErrorScope.Counter("cas_not_applied"),
	}

	metrics := &Metrics{
		JobMetrics:            jobMetrics,
		TaskMetrics:           taskMetrics,
		ResourcePoolMetrics:   resourcePoolMetrics,
		FrameworkStoreMetrics: frameworkStoreMetrics,
		VolumeMetrics:         volumeMetrics,
		ErrorMetrics:          errorMetrics,
	}

	return metrics
}
