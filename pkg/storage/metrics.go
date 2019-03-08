// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	JobNameToID     tally.Counter
	JobNameToIDFail tally.Counter

	JobGetNameToID     tally.Counter
	JobGetNameToIDFail tally.Counter

	// Timers
	JobGetByStatesDuration     tally.Timer
	JobGetByStatesFailDuration tally.Timer

	// Recovery
	ActiveJobsAddSuccess    tally.Counter
	ActiveJobsAddFail       tally.Counter
	ActiveJobsDeleteSuccess tally.Counter
	ActiveJobsDeleteFail    tally.Counter
	GetActiveJobsSuccess    tally.Counter
	GetActiveJobsFail       tally.Counter
	GetActiveJobsDuration   tally.Timer
}

// OrmJobMetrics tracks counters for job related tables accessed through ORM layer
type OrmJobMetrics struct {
	// job_index
	JobIndexCreate     tally.Counter
	JobIndexCreateFail tally.Counter
	JobIndexGet        tally.Counter
	JobIndexGetFail    tally.Counter
	JobIndexUpdate     tally.Counter
	JobIndexUpdateFail tally.Counter
	JobIndexDelete     tally.Counter
	JobIndexDeleteFail tally.Counter

	// job_name_to_id
	JobNameToIDCreate     tally.Counter
	JobNameToIDCreateFail tally.Counter
	JobNameToIDGetAll     tally.Counter
	JobNameToIDGetAllFail tally.Counter

	// job_config
	JobConfigCreate     tally.Counter
	JobConfigCreateFail tally.Counter
	JobConfigGet        tally.Counter
	JobConfigGetFail    tally.Counter
	JobConfigDelete     tally.Counter
	JobConfigDeleteFail tally.Counter

	// secret_info
	SecretInfoCreate     tally.Counter
	SecretInfoCreateFail tally.Counter
	SecretInfoGet        tally.Counter
	SecretInfoGetFail    tally.Counter
	SecretInfoUpdate     tally.Counter
	SecretInfoUpdateFail tally.Counter
	SecretInfoDelete     tally.Counter
	SecretInfoDeleteFail tally.Counter
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

	TaskGetConfig tally.Counter
	// This metric is to indicate how many task gets are performed using
	// legacy task_config table
	TaskGetConfigLegacy tally.Counter
	TaskGetConfigFail   tally.Counter

	TaskGetConfigs     tally.Counter
	TaskGetConfigsFail tally.Counter

	TaskLogState     tally.Counter
	TaskLogStateFail tally.Counter

	TaskGetLogState     tally.Counter
	TaskGetLogStateFail tally.Counter

	TaskGetForJob     tally.Counter
	TaskGetForJobFail tally.Counter

	TaskGetForJobAndStates     tally.Counter
	TaskGetForJobAndStatesFail tally.Counter

	TaskIDsGetForJobAndState     tally.Counter
	TaskIDsGetForJobAndStateFail tally.Counter

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

	TaskQueryTasks     tally.Counter
	TaskQueryTasksFail tally.Counter

	PodEventsAddSuccess tally.Counter
	PodEventsAddFail    tally.Counter

	PodEventsGetSucess tally.Counter
	PodEventsGetFail   tally.Counter

	PodEventsDeleteSucess tally.Counter
	PodEventsDeleteFail   tally.Counter
}

// UpdateMetrics is a struct for tracking job update related
// counters in the storage layer.
type UpdateMetrics struct {
	UpdateCreate     tally.Counter
	UpdateCreateFail tally.Counter

	UpdateGet     tally.Counter
	UpdateGetFail tally.Counter

	UpdateWriteProgress     tally.Counter
	UpdateWriteProgressFail tally.Counter

	UpdateGetProgess     tally.Counter
	UpdateGetProgessFail tally.Counter

	UpdateGetForJob     tally.Counter
	UpdateGetForJobFail tally.Counter

	UpdateDeleteFail tally.Counter
	UpdateDelete     tally.Counter

	JobUpdateEventAdd     tally.Counter
	JobUpdateEventAddFail tally.Counter

	JobUpdateEventGet     tally.Counter
	JobUpdateEventGetFail tally.Counter

	JobUpdateEventDelete     tally.Counter
	JobUpdateEventDeleteFail tally.Counter
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

// SecretMetrics is a struct for tracking secrets related counters in the storage layer
type SecretMetrics struct {
	SecretCreate     tally.Counter
	SecretCreateFail tally.Counter
	SecretGet        tally.Counter
	SecretGetFail    tally.Counter
	SecretUpdate     tally.Counter
	SecretUpdateFail tally.Counter
	SecretNotFound   tally.Counter
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

// WorkflowMetrics is a struct for tracking all the workflow operations/events
type WorkflowMetrics struct {
	WorkflowEventsAdd     tally.Counter
	WorkflowEventsAddFail tally.Counter

	WorkflowEventsGet     tally.Counter
	WorkflowEventsGetFail tally.Counter

	WorkflowEventsDelete     tally.Counter
	WorkflowEventsDeleteFail tally.Counter
}

// OrmTaskMetrics tracks counters for pod related tables
type OrmTaskMetrics struct {
	PodEventsAdd     tally.Counter
	PodEventsAddFail tally.Counter
	PodEventsGet     tally.Counter
	PodEventsGetFail tally.Counter
}

// Metrics is a struct for tracking all the general purpose counters that have relevance to the storage
// layer, i.e. how many jobs and tasks were created/deleted in the storage layer
type Metrics struct {
	JobMetrics            *JobMetrics
	TaskMetrics           *TaskMetrics
	UpdateMetrics         *UpdateMetrics
	ResourcePoolMetrics   *ResourcePoolMetrics
	FrameworkStoreMetrics *FrameworkStoreMetrics
	VolumeMetrics         *VolumeMetrics
	SecretMetrics         *SecretMetrics
	ErrorMetrics          *ErrorMetrics
	WorkflowMetrics       *WorkflowMetrics
	OrmJobMetrics         *OrmJobMetrics
	OrmTaskMetrics        *OrmTaskMetrics
}

// NewMetrics returns a new Metrics struct, with all metrics initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	jobScope := scope.SubScope("job")
	jobSuccessScope := jobScope.Tagged(map[string]string{"result": "success"})
	jobFailScope := jobScope.Tagged(map[string]string{"result": "fail"})
	jobNotFoundScope := jobScope.Tagged(map[string]string{"result": "not_found"})

	workflowScope := scope.SubScope("workflow")
	workflowSuccessScope := workflowScope.Tagged(map[string]string{"result": "success"})
	workflowFailScope := workflowScope.Tagged(map[string]string{"result": "fail"})

	taskScope := scope.SubScope("task")
	taskSuccessScope := taskScope.Tagged(map[string]string{"result": "success"})
	taskFailScope := taskScope.Tagged(map[string]string{"result": "fail"})
	taskNotFoundScope := taskScope.Tagged(map[string]string{"result": "not_found"})

	updateScope := scope.SubScope("update")
	updateSuccessScope := updateScope.Tagged(map[string]string{"result": "success"})
	updateFailScope := updateScope.Tagged(map[string]string{"result": "fail"})

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

	secretScope := scope.SubScope("secret")
	secretSuccessScope := secretScope.Tagged(map[string]string{"result": "success"})
	secretFailScope := secretScope.Tagged(map[string]string{"result": "fail"})
	secretNotFoundScope := secretScope.Tagged(map[string]string{"result": "not_found"})

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

		JobNameToID:     jobSuccessScope.Counter("job_name_to_id"),
		JobNameToIDFail: jobFailScope.Counter("job_name_to_id_fail"),

		JobGetNameToID:     jobSuccessScope.Counter("job_get_name_to_id"),
		JobGetNameToIDFail: jobFailScope.Counter("job_get_name_to_id_fail"),

		JobUpdateInfo:     jobSuccessScope.Counter("update_info"),
		JobUpdateInfoFail: jobFailScope.Counter("update_info"),

		JobGetByStatesDuration:     jobSuccessScope.Timer("get_job_by_state_duration"),
		JobGetByStatesFailDuration: jobFailScope.Timer("get_job_by_state_duration"),

		ActiveJobsAddSuccess:    jobSuccessScope.Counter("add_active_job"),
		ActiveJobsAddFail:       jobFailScope.Counter("add_active_job"),
		ActiveJobsDeleteSuccess: jobSuccessScope.Counter("delete_active_job"),
		ActiveJobsDeleteFail:    jobFailScope.Counter("delete_active_job"),
		GetActiveJobsSuccess:    jobSuccessScope.Counter("get_active_job"),
		GetActiveJobsFail:       jobFailScope.Counter("get_active_job"),
		GetActiveJobsDuration:   jobSuccessScope.Timer("get_active_jobs_duration"),
	}

	taskMetrics := &TaskMetrics{
		TaskCreate:           taskSuccessScope.Counter("create"),
		TaskCreateFail:       taskFailScope.Counter("create"),
		TaskCreateConfig:     taskSuccessScope.Counter("create_config"),
		TaskCreateConfigFail: taskFailScope.Counter("create_config"),
		TaskGet:              taskSuccessScope.Counter("get"),
		TaskGetFail:          taskFailScope.Counter("get"),
		TaskGetConfig:        taskSuccessScope.Counter("get_config"),
		TaskGetConfigLegacy:  taskSuccessScope.Counter("get_config_legacy"),
		TaskGetConfigFail:    taskFailScope.Counter("get_config"),
		TaskGetConfigs:       taskSuccessScope.Counter("get_configs"),
		TaskGetConfigsFail:   taskFailScope.Counter("get_configs"),

		TaskLogState:        taskSuccessScope.Counter("log_state"),
		TaskLogStateFail:    taskFailScope.Counter("log_state"),
		TaskGetLogState:     taskSuccessScope.Counter("get_log_state"),
		TaskGetLogStateFail: taskFailScope.Counter("get_log_state"),

		TaskGetForJob:                  taskSuccessScope.Counter("get_for_job"),
		TaskGetForJobFail:              taskFailScope.Counter("get_for_job"),
		TaskGetForJobAndStates:         taskSuccessScope.Counter("get_for_job_and_states"),
		TaskGetForJobAndStatesFail:     taskFailScope.Counter("get_for_job_and_states"),
		TaskIDsGetForJobAndState:       taskSuccessScope.Counter("get_ids_for_job_and_state"),
		TaskIDsGetForJobAndStateFail:   taskFailScope.Counter("get_ids_for_job_and_state"),
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
		TaskQueryTasks:        taskSuccessScope.Counter("query_tasks_for_job"),
		TaskQueryTasksFail:    taskFailScope.Counter("query_tasks_for_job"),

		TaskDelete:     taskSuccessScope.Counter("delete"),
		TaskDeleteFail: taskFailScope.Counter("delete"),
		TaskUpdate:     taskSuccessScope.Counter("update"),
		TaskUpdateFail: taskFailScope.Counter("update"),
		TaskNotFound:   taskNotFoundScope.Counter("get"),

		PodEventsAddSuccess:   taskSuccessScope.Counter("pod_events_add"),
		PodEventsAddFail:      taskFailScope.Counter("pod_events_add"),
		PodEventsGetSucess:    taskSuccessScope.Counter("pod_events_get"),
		PodEventsGetFail:      taskFailScope.Counter("pod_events_get"),
		PodEventsDeleteSucess: taskSuccessScope.Counter("pod_events_delete"),
		PodEventsDeleteFail:   taskFailScope.Counter("pod_events_delete"),
	}

	updateMetrics := &UpdateMetrics{
		UpdateCreate:     updateSuccessScope.Counter("create"),
		UpdateCreateFail: updateFailScope.Counter("create"),

		UpdateGet:     updateSuccessScope.Counter("get"),
		UpdateGetFail: updateFailScope.Counter("get"),

		UpdateWriteProgress:     updateSuccessScope.Counter("write_progress"),
		UpdateWriteProgressFail: updateFailScope.Counter("write_progress"),

		UpdateGetProgess:     updateSuccessScope.Counter("get_progress"),
		UpdateGetProgessFail: updateFailScope.Counter("get_progress"),

		UpdateGetForJob:     updateSuccessScope.Counter("get_for_job"),
		UpdateGetForJobFail: updateFailScope.Counter("get_for_job"),

		UpdateDelete:     updateSuccessScope.Counter("delete"),
		UpdateDeleteFail: updateFailScope.Counter("delete"),

		JobUpdateEventAdd:     updateSuccessScope.Counter("job_update_event_add"),
		JobUpdateEventAddFail: updateFailScope.Counter("job_update_event_add"),

		JobUpdateEventGet:     updateSuccessScope.Counter("job_update_event_get"),
		JobUpdateEventGetFail: updateFailScope.Counter("job_update_event_get"),

		JobUpdateEventDelete:     updateSuccessScope.Counter("job_update_event_delete"),
		JobUpdateEventDeleteFail: updateFailScope.Counter("job_update_event_delete"),
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

	secretMetrics := &SecretMetrics{
		SecretCreate:     secretSuccessScope.Counter("create"),
		SecretCreateFail: secretFailScope.Counter("create"),
		SecretUpdate:     secretSuccessScope.Counter("update"),
		SecretUpdateFail: secretFailScope.Counter("update"),
		SecretGet:        secretSuccessScope.Counter("get"),
		SecretGetFail:    secretFailScope.Counter("get"),
		SecretNotFound:   secretNotFoundScope.Counter("get"),
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

	workflowMetrics := &WorkflowMetrics{
		WorkflowEventsAdd:     workflowSuccessScope.Counter("add"),
		WorkflowEventsAddFail: workflowFailScope.Counter("add"),

		WorkflowEventsDelete:     workflowSuccessScope.Counter("delete"),
		WorkflowEventsDeleteFail: workflowFailScope.Counter("delete"),

		WorkflowEventsGet:     workflowSuccessScope.Counter("get"),
		WorkflowEventsGetFail: workflowFailScope.Counter("get"),
	}

	ormScope := scope.SubScope("orm")

	jobIndexScope := ormScope.SubScope("job_index")
	jobIndexSuccessScope := jobIndexScope.Tagged(
		map[string]string{"result": "success"})
	jobIndexFailScope := jobIndexScope.Tagged(
		map[string]string{"result": "fail"})

	jobNameToIDScope := ormScope.SubScope("job_name_to_id")
	jobNameToIDSuccessScope := jobNameToIDScope.Tagged(
		map[string]string{"result": "success"})
	jobNameToIDFailScope := jobNameToIDScope.Tagged(
		map[string]string{"result": "fail"})

	jobConfigScope := ormScope.SubScope("job_config")
	jobConfigSuccessScope := jobConfigScope.Tagged(
		map[string]string{"result": "success"})
	jobConfigFailScope := jobConfigScope.Tagged(
		map[string]string{"result": "fail"})

	podEventsScope := ormScope.SubScope("pod_events")
	podEventsSuccessScope := podEventsScope.Tagged(
		map[string]string{"result": "success"})
	podEventsFailScope := podEventsScope.Tagged(
		map[string]string{"result": "fail"})

	secretInfoScope := ormScope.SubScope("secret_info")
	secretInfoSuccessScope := secretInfoScope.Tagged(
		map[string]string{"result": "success"})
	secretInfoFailScope := secretInfoScope.Tagged(
		map[string]string{"result": "fail"})

	ormJobMetrics := &OrmJobMetrics{
		JobIndexCreate:     jobIndexSuccessScope.Counter("create"),
		JobIndexCreateFail: jobIndexFailScope.Counter("create"),
		JobIndexGet:        jobIndexSuccessScope.Counter("get"),
		JobIndexGetFail:    jobIndexFailScope.Counter("get"),
		JobIndexUpdate:     jobIndexSuccessScope.Counter("update"),
		JobIndexUpdateFail: jobIndexFailScope.Counter("update"),
		JobIndexDelete:     jobIndexSuccessScope.Counter("delete"),
		JobIndexDeleteFail: jobIndexFailScope.Counter("delete"),

		JobNameToIDCreate:     jobNameToIDSuccessScope.Counter("create"),
		JobNameToIDCreateFail: jobNameToIDFailScope.Counter("create"),
		JobNameToIDGetAll:     jobNameToIDSuccessScope.Counter("get_all"),
		JobNameToIDGetAllFail: jobNameToIDFailScope.Counter("get_all"),

		JobConfigCreate:     jobConfigSuccessScope.Counter("create"),
		JobConfigCreateFail: jobConfigFailScope.Counter("create"),
		JobConfigGet:        jobConfigSuccessScope.Counter("get"),
		JobConfigGetFail:    jobConfigFailScope.Counter("get"),
		JobConfigDelete:     jobConfigSuccessScope.Counter("delete"),
		JobConfigDeleteFail: jobConfigFailScope.Counter("delete"),

		SecretInfoCreate:     secretInfoSuccessScope.Counter("create"),
		SecretInfoCreateFail: secretInfoFailScope.Counter("create"),
		SecretInfoGet:        secretInfoSuccessScope.Counter("get"),
		SecretInfoGetFail:    secretInfoFailScope.Counter("get"),
		SecretInfoUpdate:     secretInfoSuccessScope.Counter("update"),
		SecretInfoUpdateFail: secretInfoFailScope.Counter("update"),
		SecretInfoDelete:     secretInfoSuccessScope.Counter("delete"),
		SecretInfoDeleteFail: secretInfoFailScope.Counter("delete"),
	}

	ormTaskMetrics := &OrmTaskMetrics{
		PodEventsAdd:     podEventsSuccessScope.Counter("add"),
		PodEventsAddFail: podEventsFailScope.Counter("add"),
		PodEventsGet:     podEventsSuccessScope.Counter("get"),
		PodEventsGetFail: podEventsFailScope.Counter("get"),
	}

	metrics := &Metrics{
		JobMetrics:            jobMetrics,
		TaskMetrics:           taskMetrics,
		UpdateMetrics:         updateMetrics,
		ResourcePoolMetrics:   resourcePoolMetrics,
		FrameworkStoreMetrics: frameworkStoreMetrics,
		VolumeMetrics:         volumeMetrics,
		SecretMetrics:         secretMetrics,
		ErrorMetrics:          errorMetrics,
		WorkflowMetrics:       workflowMetrics,
		OrmJobMetrics:         ormJobMetrics,
		OrmTaskMetrics:        ormTaskMetrics,
	}

	return metrics
}
