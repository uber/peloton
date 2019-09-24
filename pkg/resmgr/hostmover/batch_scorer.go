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

package hostmover

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	hostsvc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/lifecycle"
	rmtask "github.com/uber/peloton/pkg/resmgr/task"

	log "github.com/sirupsen/logrus"
)

const (
	// hostScorerInterval is the interval to run host scorer.
	hostScorerInterval = 5 * time.Second

	// _timeout is the time out setting to make HostMgr call
	_timeout = 5 * time.Second
)

// Scorer provides the abstraction to calculate host scores
type Scorer interface {
	// Start starts the Scorer goroutines
	Start() error
	// Stop stops the Scorer goroutines
	Stop() error

	// GetHostsByScores returns a list of hosts with the lowest scores
	GetHostsByScores(numHosts uint32) []string
}

// host Metrics of a host running batch jobs
type batchHostMetrics struct {
	// host the metrics is for
	host string
	// number of controllers running on the host
	numControllers uint32
	// number of tasks running on the host
	numTasks uint32
	// number of nonpreemptible tasks
	numNonpreemptible uint32
	// average runtime of the tasks
	tasksAvgRunTime float64

	// Resource pool id to priorities of tasks
	tasksPriority map[string][]uint32
}

type batchScorer struct {

	// enabled is the flag to enable/disable Scorer
	enabled bool

	// lifecycle manager
	lifeCycle lifecycle.LifeCycle

	// hostClient to access HostService service
	hostClient hostsvc.HostServiceYARPCClient

	// rmtasks tracker
	rmTracker rmtask.Tracker

	// mutex for the host list
	hostsLock sync.RWMutex

	// sorted host list by host scores
	orderedHosts []string
}

// NewBatchScorer creates a new batch scorer which sorts the batch hosts by
// the host scores calcuated from host metrics
func NewBatchScorer(
	enableHostScorer bool,
	hostClient hostsvc.HostServiceYARPCClient) Scorer {
	batchScorer := &batchScorer{
		lifeCycle:    lifecycle.NewLifeCycle(),
		hostClient:   hostClient,
		rmTracker:    rmtask.GetTracker(),
		orderedHosts: make([]string, 0),
	}

	batchScorer.enabled = enableHostScorer

	return batchScorer
}

func newBatchHostMetrics(hostname string) *batchHostMetrics {
	return &batchHostMetrics{
		host:          hostname,
		tasksPriority: make(map[string][]uint32),
	}
}

// Start starts Batch Scorer process
func (s *batchScorer) Start() error {
	if !s.enabled {
		log.Infof("Batch scorer is not enabled to run")
		return nil
	}

	if s.lifeCycle.Start() {
		go func() {
			defer s.lifeCycle.StopComplete()

			ticker := time.NewTicker(hostScorerInterval)
			defer ticker.Stop()

			log.Info("Starting Batch scorer")

			for {
				select {
				case <-s.lifeCycle.StopCh():
					log.Info("Exiting Batch scorer")
					return
				case <-ticker.C:
					err := s.sortOnce()
					if err != nil {
						log.WithError(err).Warn("BatchScorer cycle failed")
					}
				}
			}
		}()
	}

	return nil
}

// Stop stops Batch Scorer process
func (s *batchScorer) Stop() error {
	if !s.lifeCycle.Stop() {
		log.Warn("Batch Scorer is already stopped, no action will be performed")
		return nil
	}
	log.Info("Stopping Batch Scorer")

	// Wait for Batch Scorer to be stopped
	s.lifeCycle.Wait()
	log.Info("Batch Scorer Stopped")
	return nil
}

// sortHostsByScores sorts the hosts by host metrics
// The following metrics are used in priority order to sort the hosts:
//  any nonpreemptible tasks
//  number of controller tasks
//  number of tasks
//  aggregated normalized task priority
//  average task running time
//
func (s *batchScorer) sortOnce() error {
	log.Debug("sorting batch hosts by host scores")

	// Make call to host pool manager to get the list of batch hosts
	var batchHosts []string
	ctx, cancelFunc := context.WithTimeout(context.Background(), _timeout)
	defer cancelFunc()
	resp, err := s.hostClient.ListHostPools(
		ctx,
		&hostsvc.ListHostPoolsRequest{})

	if err != nil {
		log.WithError(err).Error("error in ListHostPools")
		return err
	}

	poolFound := false
	for _, p := range resp.GetPools() {
		if p.GetName() == common.SharedHostPoolID {
			batchHosts = append(batchHosts, p.GetHosts()...)
			poolFound = true
		}
	}
	log.WithFields(log.Fields{
		"number of batch hosts": len(batchHosts),
		"hosts":                 batchHosts,
	}).Info("all batch hosts from host manager")

	if !poolFound {
		return fmt.Errorf("pool %q not "+
			"found", common.SharedHostPoolID)
	}

	hostTasks := s.rmTracker.TasksByHosts(batchHosts, resmgr.TaskType_BATCH)
	currentTime := time.Now()

	var noTasksHosts []string
	for _, batchHost := range batchHosts {
		if _, ok := hostTasks[batchHost]; !ok {
			noTasksHosts = append(noTasksHosts, batchHost)
		}
	}

	log.WithFields(log.Fields{
		"number of hosts": len(batchHosts),
		"hosts":           batchHosts,
	}).Info("all batch hosts which does not have tasks")

	hostMetrics := make([]*batchHostMetrics, len(hostTasks))
	index := 0
	for host, tasksPerHost := range hostTasks {
		hostMetrics[index] = newBatchHostMetrics(host)

		for _, task := range tasksPerHost {
			if task.GetCurrentState().State == pbtask.TaskState_LAUNCHED ||
				task.GetCurrentState().State == pbtask.TaskState_STARTING ||
				task.GetCurrentState().State == pbtask.TaskState_RUNNING {
				// Update metrics
				if task.Task().Controller {
					hostMetrics[index].numControllers++
				}

				if !task.Task().Preemptible {
					hostMetrics[index].numNonpreemptible++
				}

				hostMetrics[index].numTasks++

				if tasksPriority, ok := hostMetrics[index].tasksPriority[task.Respool().ID()]; !ok {
					tasksPriority = make([]uint32, 0)
					tasksPriority = append(tasksPriority, task.Task().Priority)
					hostMetrics[index].tasksPriority[task.Respool().ID()] = tasksPriority
				} else {
					tasksPriority = append(tasksPriority, task.Task().Priority)
					hostMetrics[index].tasksPriority[task.Respool().ID()] = tasksPriority
				}

				hostMetrics[index].tasksAvgRunTime += float64(currentTime.Sub(task.RunTimeStats().StartTime))
			}
		}
		if len(tasksPerHost) > 0 {
			hostMetrics[index].tasksAvgRunTime /= float64(len(tasksPerHost))
		}

		index++
	}

	// Normalize the task priorities
	// The priorities in each resource pool are normalized to
	// same scale and aggregated
	poolMaxPriorities := make(map[string]float64)
	clusterMaxPriority := 0.0

	for _, metrics := range hostMetrics {
		for poolID, priorities := range metrics.tasksPriority {
			for _, priority := range priorities {
				clusterMaxPriority = math.Max(clusterMaxPriority, float64(priority))

				if _, ok := poolMaxPriorities[poolID]; !ok {
					poolMaxPriorities[poolID] = float64(priority)
				} else {
					poolMaxPriorities[poolID] = math.Max(poolMaxPriorities[poolID], float64(priority))
				}
			}
		}
	}

	// Aggregated normalized priority of all the tasks
	aggrPriorities := make([]float64, len(hostMetrics))
	for index, metrics := range hostMetrics {
		for poolID, priorities := range metrics.tasksPriority {
			poolMaxPriority := poolMaxPriorities[poolID]
			ratio := 0.0
			if poolMaxPriority != 0 {
				ratio = clusterMaxPriority / poolMaxPriority
			}

			for _, priority := range priorities {
				aggrPriorities[index] += float64(priority) * ratio
			}
		}
	}

	// Sort the hosts by the metrics
	sort.Slice(hostMetrics, func(i, j int) bool {
		if hostMetrics[i].numNonpreemptible != hostMetrics[j].numNonpreemptible {
			return hostMetrics[i].numNonpreemptible < hostMetrics[j].numNonpreemptible
		}
		if hostMetrics[i].numControllers != hostMetrics[j].numControllers {
			return hostMetrics[i].numControllers < hostMetrics[j].numControllers
		}
		if hostMetrics[i].numTasks != hostMetrics[j].numTasks {
			return hostMetrics[i].numTasks < hostMetrics[j].numTasks
		}
		if aggrPriorities[i] != aggrPriorities[j] {
			return aggrPriorities[i] < aggrPriorities[j]
		}
		if hostMetrics[i].tasksAvgRunTime != hostMetrics[j].tasksAvgRunTime {
			return hostMetrics[i].tasksAvgRunTime < hostMetrics[j].tasksAvgRunTime
		}
		return true
	})

	//hosts := make([]string, 0, len(hostMetrics))
	var hosts []string
	for _, metrics := range hostMetrics {
		if metrics.numNonpreemptible == 0 {
			hosts = append(hosts, metrics.host)
		}
	}

	log.WithFields(log.Fields{
		"no task hosts":      noTasksHosts,
		"len no task hosts ": len(noTasksHosts),
		"taks hosts":         hosts,
		"len task hosts":     len(hosts),
	}).Info("hosts breakdown before appending")

	// Append hosts with no task running on top of the list
	hosts = append(noTasksHosts, hosts...)
	// copy to target hosts array
	s.hostsLock.Lock()
	defer s.hostsLock.Unlock()

	log.WithFields(log.Fields{
		"all hosts":          hosts,
		"len no task hosts ": len(hosts),
	}).Info("final hosts list")

	s.orderedHosts = hosts

	return nil
}

func (s *batchScorer) GetHostsByScores(numHosts uint32) []string {
	s.hostsLock.RLock()
	defer s.hostsLock.RUnlock()

	size := uint(math.Min(float64(numHosts), float64(len(s.orderedHosts))))

	if size == 0 {
		return []string{}
	}

	hosts := make([]string, size)
	copy(hosts, s.orderedHosts)

	return hosts
}
