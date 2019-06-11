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

package handler

import (
	"context"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"

	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"
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
	jobConfigOps ormobjects.JobConfigOps) (jobmgrcommon.JobConfig, error) {
	cachedJob := factory.GetJob(id)
	if cachedJob != nil {
		return cachedJob.GetConfig(ctx)
	}

	jobConfig, _, err := jobConfigOps.GetCurrentVersion(ctx, id)
	return jobConfig, err
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
	jobRuntimeOps ormobjects.JobRuntimeOps) (*job.RuntimeInfo, error) {
	cachedJob := factory.GetJob(id)
	if cachedJob != nil {
		return cachedJob.GetRuntime(ctx)
	}

	return jobRuntimeOps.Get(ctx, id)
}
