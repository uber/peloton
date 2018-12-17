package aurorabridge

import (
	"context"
	"errors"

	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api"
	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api/auroraschedulermanagerserver"
	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api/readonlyschedulerserver"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

var errUnimplemented = errors.New("rpc is unimplemented")

// ServiceHandler implements a partial Aurora API. Various unneeded methods have
// been left intentionally unimplemented.
type serviceHandler struct {
	metrics *Metrics
}

// NewServiceHandler returns a new ServiceHandler.
func NewServiceHandler(
	parent tally.Scope,
	d *yarpc.Dispatcher) {
	handler := &serviceHandler{
		metrics: NewMetrics(parent.SubScope("aurorabridge").SubScope("api")),
	}
	d.Register(auroraschedulermanagerserver.New(handler))
	d.Register(readonlyschedulerserver.New(handler))
}

// GetJobSummary returns a summary of jobs, optionally only those owned by a specific role.
func (h *serviceHandler) GetJobSummary(
	context context.Context,
	role *string) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetTasksWithoutConfigs is the same as getTaskStatus but without the TaskConfig.ExecutorConfig
// data set.
func (h *serviceHandler) GetTasksWithoutConfigs(
	context context.Context,
	query *api.TaskQuery) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetConfigSummary fetches the configuration summary of active tasks for the specified job.
func (h *serviceHandler) GetConfigSummary(
	context context.Context,
	job *api.JobKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetJobs fetches the status of jobs. ownerRole is optional, in which case all jobs are returned.
func (h *serviceHandler) GetJobs(
	context context.Context,
	ownerRole *string) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetJobUpdateSummaries gets job update summaries.
func (h *serviceHandler) GetJobUpdateSummaries(
	context context.Context,
	jobUpdateQuery *api.JobUpdateQuery) (*api.Response, error) {

	return nil, errUnimplemented
}

// GetJobUpdateDetails gets job update details.
func (h *serviceHandler) GetJobUpdateDetails(
	context context.Context,
	key *api.JobUpdateKey,
	query *api.JobUpdateQuery) (*api.Response, error) {

	return nil, errUnimplemented
}

// GetJobUpdateDiff gets the diff between client (desired) and server (current) job states.
func (h *serviceHandler) GetJobUpdateDiff(
	context context.Context,
	request *api.JobUpdateRequest) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetTierConfigs is a no-op. It is only used to determine liveness of the scheduler.
func (h *serviceHandler) GetTierConfigs(
	context context.Context) (*api.Response, error) {
	return nil, errUnimplemented
}

// KillTasks initiates a kill on tasks.
func (h *serviceHandler) KillTasks(
	context context.Context,
	job *api.JobKey,
	instances map[int32]struct{},
	message *string) (*api.Response, error) {

	return nil, errUnimplemented
}

// StartJobUpdate starts update of the existing service job.
func (h *serviceHandler) StartJobUpdate(
	context context.Context,
	request *api.JobUpdateRequest,
	message *string) (*api.Response, error) {

	return nil, errUnimplemented
}

// PauseJobUpdate pauses the specified job update. Can be resumed by resumeUpdate call.
func (h *serviceHandler) PauseJobUpdate(
	context context.Context,
	key *api.JobUpdateKey,
	message *string) (*api.Response, error) {

	return nil, errUnimplemented
}

// ResumeJobUpdate resumes progress of a previously paused job update.
func (h *serviceHandler) ResumeJobUpdate(
	context context.Context,
	key *api.JobUpdateKey, message *string) (*api.Response, error) {

	return nil, errUnimplemented
}

// AbortJobUpdate permanently aborts the job update. Does not remove the update history.
func (h *serviceHandler) AbortJobUpdate(
	context context.Context,
	key *api.JobUpdateKey,
	message *string) (*api.Response, error) {

	return nil, errUnimplemented
}

// RollbackJobUpdate rollbacks the specified active job update to the initial state.
func (h *serviceHandler) RollbackJobUpdate(
	context context.Context,
	key *api.JobUpdateKey,
	message *string) (*api.Response, error) {

	return nil, errUnimplemented
}

// PulseJobUpdate allows progress of the job update in case blockIfNoPulsesAfterMs is specified in
// JobUpdateSettings. Unblocks progress if the update was previously blocked.
// Responds with ResponseCode.INVALID_REQUEST in case an unknown update key is specified.
func (h *serviceHandler) PulseJobUpdate(
	context context.Context,
	key *api.JobUpdateKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// ==== UNUSED RPCS ====

// GetRoleSummary will remain unimplemented.
func (h *serviceHandler) GetRoleSummary(
	context context.Context) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetTasksStatus will remain unimplemented.
func (h *serviceHandler) GetTasksStatus(
	context context.Context,
	query *api.TaskQuery) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetPendingReason will remain unimplemented.
func (h *serviceHandler) GetPendingReason(
	context context.Context,
	query *api.TaskQuery) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetQuota will remain unimplemented.
func (h *serviceHandler) GetQuota(
	context context.Context,
	ownerRole *string) (*api.Response, error) {
	return nil, errUnimplemented
}

// PopulateJobConfig will remain unimplemented.
func (h *serviceHandler) PopulateJobConfig(
	context context.Context,
	description *api.JobConfiguration) (*api.Response, error) {

	return nil, errUnimplemented
}

// CreateJob will remain unimplemented.
func (h *serviceHandler) CreateJob(
	context context.Context,
	description *api.JobConfiguration) (*api.Response, error) {
	return nil, errUnimplemented
}

// ScheduleCronJob will remain unimplemented.
func (h *serviceHandler) ScheduleCronJob(
	context context.Context,
	description *api.JobConfiguration) (*api.Response, error) {
	return nil, errUnimplemented
}

// DescheduleCronJob will remain unimplemented.
func (h *serviceHandler) DescheduleCronJob(
	context context.Context,
	job *api.JobKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// StartCronJob will remain unimplemented.
func (h *serviceHandler) StartCronJob(
	context context.Context,
	job *api.JobKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// RestartShards will remain unimplemented.
func (h *serviceHandler) RestartShards(
	context context.Context,
	job *api.JobKey,
	shardIds map[int32]struct{}) (*api.Response, error) {

	return nil, errUnimplemented
}

// AddInstances will remain unimplemented.
func (h *serviceHandler) AddInstances(
	context context.Context,
	key *api.InstanceKey,
	count *int32) (*api.Response, error) {
	return nil, errUnimplemented
}

// ReplaceCronTemplate will remain unimplemented.
func (h *serviceHandler) ReplaceCronTemplate(
	context context.Context,
	config *api.JobConfiguration) (*api.Response, error) {
	return nil, errUnimplemented
}
