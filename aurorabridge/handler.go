package aurorabridge

import (
	"errors"

	"code.uber.internal/infra/peloton/aurorabridge/aurora/api"
	"github.com/uber-go/tally"
)

var errUnimplemented = errors.New("rpc is unimplemented")

// ServiceHandler implements a partial Aurora API. Various unneeded methods have
// been left intentionally unimplemented.
type ServiceHandler struct {
	scope tally.Scope
}

// NewServiceHandler returns a new ServiceHandler.
func NewServiceHandler(scope tally.Scope) *ServiceHandler {
	return &ServiceHandler{scope}
}

// GetJobSummary returns a summary of jobs, optionally only those owned by a specific role.
func (h *ServiceHandler) GetJobSummary(role string) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetTasksWithoutConfigs is the same as getTaskStatus but without the TaskConfig.ExecutorConfig
// data set.
func (h *ServiceHandler) GetTasksWithoutConfigs(query *api.TaskQuery) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetConfigSummary fetches the configuration summary of active tasks for the specified job.
func (h *ServiceHandler) GetConfigSummary(job *api.JobKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetJobs fetches the status of jobs. ownerRole is optional, in which case all jobs are returned.
func (h *ServiceHandler) GetJobs(ownerRole string) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetJobUpdateSummaries gets job update summaries.
func (h *ServiceHandler) GetJobUpdateSummaries(
	jobUpdateQuery *api.JobUpdateQuery) (*api.Response, error) {

	return nil, errUnimplemented
}

// GetJobUpdateDetails gets job update details.
func (h *ServiceHandler) GetJobUpdateDetails(
	key *api.JobUpdateKey, query *api.JobUpdateQuery) (*api.Response, error) {

	return nil, errUnimplemented
}

// GetJobUpdateDiff gets the diff between client (desired) and server (current) job states.
func (h *ServiceHandler) GetJobUpdateDiff(request *api.JobUpdateRequest) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetTierConfigs is a no-op. It is only used to determine liveness of the scheduler.
func (h *ServiceHandler) GetTierConfigs() (*api.Response, error) {
	return nil, errUnimplemented
}

// KillTasks initiates a kill on tasks.
func (h *ServiceHandler) KillTasks(
	job *api.JobKey, instances map[int32]struct{}, message string) (*api.Response, error) {

	return nil, errUnimplemented
}

// StartJobUpdate starts update of the existing service job.
func (h *ServiceHandler) StartJobUpdate(
	request *api.JobUpdateRequest, message string) (*api.Response, error) {

	return nil, errUnimplemented
}

// PauseJobUpdate pauses the specified job update. Can be resumed by resumeUpdate call.
func (h *ServiceHandler) PauseJobUpdate(
	key *api.JobUpdateKey, message string) (*api.Response, error) {

	return nil, errUnimplemented
}

// ResumeJobUpdate resumes progress of a previously paused job update.
func (h *ServiceHandler) ResumeJobUpdate(
	key *api.JobUpdateKey, message string) (*api.Response, error) {

	return nil, errUnimplemented
}

// AbortJobUpdate permanently aborts the job update. Does not remove the update history.
func (h *ServiceHandler) AbortJobUpdate(
	key *api.JobUpdateKey, message string) (*api.Response, error) {

	return nil, errUnimplemented
}

// RollbackJobUpdate rollbacks the specified active job update to the initial state.
func (h *ServiceHandler) RollbackJobUpdate(
	key *api.JobUpdateKey, message string) (*api.Response, error) {

	return nil, errUnimplemented
}

// PulseJobUpdate allows progress of the job update in case blockIfNoPulsesAfterMs is specified in
// JobUpdateSettings. Unblocks progress if the update was previously blocked.
// Responds with ResponseCode.INVALID_REQUEST in case an unknown update key is specified.
func (h *ServiceHandler) PulseJobUpdate(key *api.JobUpdateKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// ==== UNUSED RPCS ====

// GetRoleSummary will remain unimplemented.
func (h *ServiceHandler) GetRoleSummary() (*api.Response, error) {
	return nil, errUnimplemented
}

// GetTasksStatus will remain unimplemented.
func (h *ServiceHandler) GetTasksStatus(query *api.TaskQuery) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetPendingReason will remain unimplemented.
func (h *ServiceHandler) GetPendingReason(query *api.TaskQuery) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetQuota will remain unimplemented.
func (h *ServiceHandler) GetQuota(ownerRole string) (*api.Response, error) {
	return nil, errUnimplemented
}

// PopulateJobConfig will remain unimplemented.
func (h *ServiceHandler) PopulateJobConfig(
	description *api.JobConfiguration) (*api.Response, error) {

	return nil, errUnimplemented
}

// CreateJob will remain unimplemented.
func (h *ServiceHandler) CreateJob(description *api.JobConfiguration) (*api.Response, error) {
	return nil, errUnimplemented
}

// ScheduleCronJob will remain unimplemented.
func (h *ServiceHandler) ScheduleCronJob(description *api.JobConfiguration) (*api.Response, error) {
	return nil, errUnimplemented
}

// DescheduleCronJob will remain unimplemented.
func (h *ServiceHandler) DescheduleCronJob(job *api.JobKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// StartCronJob will remain unimplemented.
func (h *ServiceHandler) StartCronJob(job *api.JobKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// RestartShards will remain unimplemented.
func (h *ServiceHandler) RestartShards(
	job *api.JobKey, shardIds map[int32]struct{}) (*api.Response, error) {

	return nil, errUnimplemented
}

// AddInstances will remain unimplemented.
func (h *ServiceHandler) AddInstances(key *api.InstanceKey, count int32) (*api.Response, error) {
	return nil, errUnimplemented
}

// ReplaceCronTemplate will remain unimplemented.
func (h *ServiceHandler) ReplaceCronTemplate(config *api.JobConfiguration) (*api.Response, error) {
	return nil, errUnimplemented
}
