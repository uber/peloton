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

package cache

import (
	"sync"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
)

// entry is a single entry inside JobIDCache
type entry struct {
	jobID string
}

// jobIDCache is implementation of JobIDCache interface.
type jobIDCache struct {
	sync.RWMutex

	cache map[string][]*entry
}

// JobIDCache is a cache to map between Aurora JobKey and Peloton ID,
// cache is keyed using JobKey role.
type JobIDCache interface {
	// GetJobIDs retrieves the ids from cache based on input Aurora JobKey
	// role parameter.
	GetJobIDs(role string) []*peloton.JobID

	// PopulateFromJobCache populates the JobIDCache based on the QueryJobCache
	// result for a particular Aurora JobKey role.
	PopulateFromJobCache(
		role string,
		jobCache []*jobmgrsvc.QueryJobCacheResponse_JobCache,
	)

	// Invalidate invalidates the cache entries based on a particular Aurora
	// JobKey role.
	Invalidate(role string)
}

func NewJobIDCache() JobIDCache {
	return &jobIDCache{
		cache: make(map[string][]*entry),
	}
}

func (c *jobIDCache) get(role string) ([]*entry, bool) {
	entries, ok := c.cache[role]
	return entries, ok
}

func (c *jobIDCache) write(role string, entries []*entry) {
	c.cache[role] = entries
}

func (c *jobIDCache) delete(role string) {
	delete(c.cache, role)
}

// GetJobIDs retrieves the ids from cache based on input Aurora JobKey
// role parameter.
func (c *jobIDCache) GetJobIDs(role string) []*peloton.JobID {
	if len(role) == 0 {
		return nil
	}

	c.RLock()
	defer c.RUnlock()

	entries, ok := c.get(role)
	if !ok {
		return nil
	}

	jobIDs := make([]*peloton.JobID, 0, len(entries))
	for _, e := range entries {
		jobIDs = append(jobIDs, &peloton.JobID{Value: e.jobID})
	}
	return jobIDs
}

// PopulateFromJobCache populates the JobIDCache based on the QueryJobCache
// result for a particular Aurora JobKey role.
func (c *jobIDCache) PopulateFromJobCache(
	role string,
	jobCache []*jobmgrsvc.QueryJobCacheResponse_JobCache,
) {
	c.Lock()
	defer c.Unlock()

	entries := make([]*entry, 0, len(jobCache))
	for _, cache := range jobCache {
		entries = append(entries, &entry{jobID: cache.GetJobId().GetValue()})
	}

	c.write(role, entries)
}

// Invalidate invalidates the cache entries based on a particular Aurora
// JobKey role.
func (c *jobIDCache) Invalidate(role string) {
	c.Lock()
	defer c.Unlock()

	c.delete(role)
}
