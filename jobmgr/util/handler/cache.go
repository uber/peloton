package handler

import (
	"context"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	"code.uber.internal/infra/peloton/storage"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
)

// GetJobConfigWithoutFillingCache returns models.JobConfig without filling in
// cache. It would first try to find the object from cache. If cache misses,
// it will load from DB.
// The function is intended to be used for API that reads jobs without
// sending the jobs to goal states.
func GetJobConfigWithoutFillingCache(
	ctx context.Context,
	id *peloton.JobID,
	factory cached.JobFactory,
	store storage.JobStore) (jobmgrcommon.JobConfig, error) {
	cachedJob := factory.GetJob(id)
	if cachedJob != nil {
		return cachedJob.GetConfig(ctx)
	}

	return store.GetJobConfig(ctx, id)
}

// GetJobRuntimeWithoutFillingCache returns job.RuntimeInfo without filling in
// cache. It would first try to find the object from cache. If cache misses,
// it will load from DB.
// The function is intended to be used for API that reads jobs without
// sending the jobs to goal states.
func GetJobRuntimeWithoutFillingCache(
	ctx context.Context,
	id *peloton.JobID,
	factory cached.JobFactory,
	store storage.JobStore) (*job.RuntimeInfo, error) {
	cachedJob := factory.GetJob(id)
	if cachedJob != nil {
		return cachedJob.GetRuntime(ctx)
	}

	return store.GetJobRuntime(ctx, id)
}
