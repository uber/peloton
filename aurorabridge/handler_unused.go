package aurorabridge

import (
	"context"

	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api"
)

// This file contains the unused RPCs which we will not be implementing, but are
// required to fulfill the Aurora interface. Placed in this separate file to
// avoid unnecessary bloat in handler.go.

// GetRoleSummary will remain unimplemented.
func (h *ServiceHandler) GetRoleSummary(
	ctx context.Context) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetTasksStatus will remain unimplemented.
func (h *ServiceHandler) GetTasksStatus(
	ctx context.Context,
	query *api.TaskQuery) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetPendingReason will remain unimplemented.
func (h *ServiceHandler) GetPendingReason(
	ctx context.Context,
	query *api.TaskQuery) (*api.Response, error) {
	return nil, errUnimplemented
}

// GetQuota will remain unimplemented.
func (h *ServiceHandler) GetQuota(
	ctx context.Context,
	ownerRole *string) (*api.Response, error) {
	return nil, errUnimplemented
}

// PopulateJobConfig will remain unimplemented.
func (h *ServiceHandler) PopulateJobConfig(
	ctx context.Context,
	description *api.JobConfiguration) (*api.Response, error) {

	return nil, errUnimplemented
}

// CreateJob will remain unimplemented.
func (h *ServiceHandler) CreateJob(
	ctx context.Context,
	description *api.JobConfiguration) (*api.Response, error) {
	return nil, errUnimplemented
}

// ScheduleCronJob will remain unimplemented.
func (h *ServiceHandler) ScheduleCronJob(
	ctx context.Context,
	description *api.JobConfiguration) (*api.Response, error) {
	return nil, errUnimplemented
}

// DescheduleCronJob will remain unimplemented.
func (h *ServiceHandler) DescheduleCronJob(
	ctx context.Context,
	job *api.JobKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// StartCronJob will remain unimplemented.
func (h *ServiceHandler) StartCronJob(
	ctx context.Context,
	job *api.JobKey) (*api.Response, error) {
	return nil, errUnimplemented
}

// RestartShards will remain unimplemented.
func (h *ServiceHandler) RestartShards(
	ctx context.Context,
	job *api.JobKey,
	shardIds map[int32]struct{}) (*api.Response, error) {

	return nil, errUnimplemented
}

// AddInstances will remain unimplemented.
func (h *ServiceHandler) AddInstances(
	ctx context.Context,
	key *api.InstanceKey,
	count *int32) (*api.Response, error) {
	return nil, errUnimplemented
}

// ReplaceCronTemplate will remain unimplemented.
func (h *ServiceHandler) ReplaceCronTemplate(
	ctx context.Context,
	config *api.JobConfiguration) (*api.Response, error) {
	return nil, errUnimplemented
}
