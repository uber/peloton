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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"

	"github.com/stretchr/testify/assert"
)

// TestJobIDCache tests basic functionality of JobIDCache
func TestJobIDCache(t *testing.T) {
	cache := NewJobIDCache()
	jobCache := []*jobmgrsvc.QueryJobCacheResponse_JobCache{
		{
			JobId: &peloton.JobID{Value: "job-id-1"},
		},
		{
			JobId: &peloton.JobID{Value: "job-id-2"},
		},
	}

	// Empty cache, expect GetJobIDs to return not exists
	ids := cache.GetJobIDs("test-role-1")
	assert.Nil(t, ids)

	// Populate cache
	cache.PopulateFromJobCache("test-role-1", jobCache)

	// Cache populated, expect GetJobIDs to return exists for role in cache
	ids = cache.GetJobIDs("test-role-1")
	assert.Equal(t, []*peloton.JobID{
		{Value: "job-id-1"},
		{Value: "job-id-2"},
	}, ids)

	// Cache populated, expect GetJobIDs to return not exists for role not
	// in cache
	ids = cache.GetJobIDs("test-role-2")
	assert.Nil(t, ids)

	// Attempt invalidate role not in cache
	cache.Invalidate("test-role-2")

	// Cache populated, expect GetJobIDs to return exists for role in cache
	ids = cache.GetJobIDs("test-role-1")
	assert.Equal(t, []*peloton.JobID{
		{Value: "job-id-1"},
		{Value: "job-id-2"},
	}, ids)

	// Attempt invalidate role in cache
	cache.Invalidate("test-role-1")

	// Empty cache, expect GetJobIDs to return not exists
	ids = cache.GetJobIDs("test-role-1")
	assert.Nil(t, ids)
}

// TestJobIDCacheConcurrency is basic concurrency test for JobIDCache
func TestJobIDCacheConcurrency(t *testing.T) {
	wg := sync.WaitGroup{}
	cache := NewJobIDCache()
	result := make([]bool, 500)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 5; i++ {
			iterWg := sync.WaitGroup{}
			for j := 0; j < 100; j++ {
				iterWg.Add(1)
				go func(i, j int) {
					defer iterWg.Done()
					jobIDs := cache.GetJobIDs(strconv.Itoa(j))
					if len(jobIDs) > 0 {
						assert.Len(t, jobIDs, 1)
						assert.Equal(t, strconv.Itoa(j), jobIDs[0].GetValue())
						result[j+i*100] = true
					}
				}(i, j)
			}
			iterWg.Wait()
			time.Sleep(200 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		time.Sleep(200 * time.Millisecond)
		for i := 0; i < 100; i++ {
			cache.PopulateFromJobCache(strconv.Itoa(i), []*jobmgrsvc.QueryJobCacheResponse_JobCache{
				{
					JobId: &peloton.JobID{Value: strconv.Itoa(i)},
				},
			})
		}
		for i := 0; i < 20; i++ {
			cache.Invalidate(strconv.Itoa(i))
		}
		for i := 0; i < 20; i++ {
			cache.PopulateFromJobCache(strconv.Itoa(i), []*jobmgrsvc.QueryJobCacheResponse_JobCache{
				{
					JobId: &peloton.JobID{Value: strconv.Itoa(i)},
				},
			})
		}
	}()

	wg.Wait()

	// Check at least some of the iterations should have cache hit
	cacheHit := func() bool {
		for _, r := range result {
			if r {
				return true
			}
		}
		return false
	}()
	assert.True(t, cacheHit)
}
