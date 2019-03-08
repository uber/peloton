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

package activermtask

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/jobmgr/cached"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

// ActiveRMTasks is the entrypoint object into the cache which store active tasks from ResMgr.
type ActiveRMTasks interface {
	// GetTask returns the task entry for the given taskID
	GetTask(taskID string) *resmgrsvc.GetActiveTasksResponse_TaskEntry

	// UpdateActiveTasks fills the cache with all tasks from Resmgr
	UpdateActiveTasks()
}

// activeTasksCache is the implementation of ActiveTasksCache
type activeRMTasks struct {
	sync.RWMutex
	// taskCache is the in-memory cache with key: taskID, value: taskEntry
	taskCache map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntry
	// resmgrClient is the Resource Manager Client
	resmgrClient resmgrsvc.ResourceManagerServiceYARPCClient
	// metrics is the metrics for ActiveRMTasks
	metrics *Metrics
}

// NewActiveRMTasks is the constructor of ActiveTasksCache
func NewActiveRMTasks(
	d *yarpc.Dispatcher,
	parent tally.Scope,
) ActiveRMTasks {
	taskCache := make(map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntry)
	return &activeRMTasks{
		resmgrClient: resmgrsvc.NewResourceManagerServiceYARPCClient(
			d.ClientConfig(common.PelotonResourceManager)),
		taskCache: taskCache,
		metrics:   NewMetrics(parent.SubScope("jobmgr").SubScope("activermtask")),
	}
}

// GetTask finds the task with taskID
func (cache *activeRMTasks) GetTask(
	taskID string) *resmgrsvc.GetActiveTasksResponse_TaskEntry {
	cache.RLock()
	defer cache.RUnlock()
	return cache.taskCache[taskID]
}

// UpdateActiveTasks fills the cache with all tasks from Resmgr
func (cache *activeRMTasks) UpdateActiveTasks() {
	callStart := time.Now()

	taskCache := make(map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntry)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()

	// TODO: resmgrsvc.GetActiveTasksRequest.States takes a slice of TaskState instead of string
	states := cached.GetResourceManagerProcessingStates()
	sort.Strings(states)
	rmResp, err := cache.resmgrClient.GetActiveTasks(
		ctx,
		&resmgrsvc.GetActiveTasksRequest{
			States: states,
		})
	if err != nil {
		// record failed
		cache.metrics.ActiveTaskQueryFail.Inc(1)
		log.WithError(err).
			Info("failed to get active tasks from ResourceManager")
		return
	}

	// iterate through the result
	for _, taskEntries := range rmResp.GetTasksByState() {
		for _, taskEntry := range taskEntries.GetTaskEntry() {
			taskID := taskEntry.GetTaskID()
			taskCache[taskID] = taskEntry
		}
	}

	// update the cache
	cache.Lock()
	defer cache.Unlock()
	cache.taskCache = taskCache

	callDuration := time.Since(callStart)
	cache.metrics.UpdaterRMTasksDuraion.Record(callDuration)

	// record succeed
	cache.metrics.ActiveTaskQuerySuccess.Inc(1)
}
