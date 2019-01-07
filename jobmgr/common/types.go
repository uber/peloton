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

package common

import (
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
)

// JobConfig stores the job configurations in cache which is fetched multiple
// times during normal job/task operations.
// JobConfig makes the job interface cleaner by having the caller request
// for the configuration first (which can fail due to Cassandra errors
// if cache is invalid or not populated yet), and then fetch the needed
// configuration from the interface. Otherwise, caller needs to deal with
// context and err for each config related call.
// The interface exposes get methods only so that the caller cannot
// overwrite any of these configurations.
type JobConfig interface {
	// GetInstanceCount returns the instance count
	// in the job config stored in the cache
	GetInstanceCount() uint32
	// GetType returns the type of the job stored in the cache
	GetType() pbjob.JobType
	// GetRespoolID returns the respool id stored in the cache
	GetRespoolID() *peloton.ResourcePoolID
	// GetSLA returns the SLA configuration
	// in the job config stored in the cache
	GetSLA() *pbjob.SlaConfig
	// GetChangeLog returns the changeLog in the job config stored in the cache
	GetChangeLog() *peloton.ChangeLog
}

// RuntimeDiff to be applied to the runtime struct.
// key is the field name to be updated,
// value is the value to be updated to.
type RuntimeDiff map[string]interface{}
