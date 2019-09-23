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

package aurorabridge

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	v0peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/cache"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/label"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"
	"github.com/uber/peloton/pkg/aurorabridge/ptoa"
	"github.com/uber/peloton/pkg/common/concurrency"
	"github.com/uber/peloton/pkg/common/util"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/thriftrw/ptr"
	"go.uber.org/yarpc/yarpcerrors"
)

var errUnimplemented = errors.New("rpc is unimplemented")

const (
	// Minimum task query depth when PodRunsDepth in the config is set to
	// larger than and equals to 2 and bounded by PodRunsMax.
	minPodRunsDepth = 2
)

// jobCache is an internal struct used to capture job id and name
// of the a specific job. Mostly used as the job query return result.
type jobCache struct {
	JobId *peloton.JobID
	Name  string
}

// ServiceHandler implements a partial Aurora API. Various unneeded methods have
// been left intentionally unimplemented.
type ServiceHandler struct {
	config        ServiceHandlerConfig
	metrics       *Metrics
	jobClient     statelesssvc.JobServiceYARPCClient
	jobmgrClient  jobmgrsvc.JobManagerServiceYARPCClient
	podClient     podsvc.PodServiceYARPCClient
	respoolLoader RespoolLoader
	random        common.Random
	jobIdCache    cache.JobIDCache
}

// NewServiceHandler creates a new ServiceHandler.
func NewServiceHandler(
	config ServiceHandlerConfig,
	parent tally.Scope,
	jobClient statelesssvc.JobServiceYARPCClient,
	jobmgrClient jobmgrsvc.JobManagerServiceYARPCClient,
	podClient podsvc.PodServiceYARPCClient,
	respoolLoader RespoolLoader,
	random common.Random,
	jobIdCache cache.JobIDCache,
) (*ServiceHandler, error) {

	config.normalize()
	if err := config.validate(); err != nil {
		return nil, err
	}

	return &ServiceHandler{
		config:        config,
		metrics:       NewMetrics(parent.SubScope("aurorabridge").SubScope("api")),
		jobClient:     jobClient,
		jobmgrClient:  jobmgrClient,
		podClient:     podClient,
		respoolLoader: respoolLoader,
		random:        random,
		jobIdCache:    jobIdCache,
	}, nil
}

// GetJobSummary returns a summary of jobs, optionally only those owned by a specific role.
func (h *ServiceHandler) GetJobSummary(
	ctx context.Context,
	role *string,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.getJobSummary(ctx, role)
	resp := newResponse(result, err, "getJobSummary")

	defer func() {
		h.metrics.
			Procedures[ProcedureGetJobSummary].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureGetJobSummary].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"role": role,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("GetJobSummary error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"role": role,
			},
			"result": result,
		}).Debug("GetJobSummary success")
	}()

	return resp, nil
}

func (h *ServiceHandler) getJobSummary(
	ctx context.Context,
	role *string,
) (*api.Result, *auroraError) {
	var jobIDs []*peloton.JobID
	var err error

	if role != nil && *role != "" {
		jobIDs, err = h.getJobIDsFromRoleCache(ctx, *role)
	} else {
		jobIDs, err = h.queryJobIDs(ctx, "", "", "")
	}

	if err != nil {
		return nil, auroraErrorf("get job ids from role: %s", err)
	}

	var inputs []interface{}
	for _, j := range jobIDs {
		inputs = append(inputs, j)
	}

	f := func(ctx context.Context, input interface{}) (interface{}, error) {
		jobID := input.(*peloton.JobID)
		jobInfo, err := h.getJobInfo(ctx, jobID)
		if err != nil {
			if yarpcerrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("get job info for job id %q: %s",
				jobID.GetValue(), err)
		}

		// If the job is stopped, skip it from the response
		if jobInfo.GetSpec().GetInstanceCount() == 0 {
			return nil, nil
		}

		// In Aurora, JobSummary.JobConfiguration.TaskConfig
		// is generated using latest "active" task. Reference:
		// https://github.com/apache/aurora/blob/master/src/main/java/org/apache/aurora/scheduler/base/Tasks.java#L133
		// but use JobInfo.JobSpec.DefaultSpec here to simplify
		// the querying logic.
		// TODO(kevinxu): Need to match Aurora's behavior?
		// TODO(kevinxu): Need to inspect InstanceSpec as well?
		podSpec := jobInfo.GetSpec().GetDefaultSpec()

		s, err := ptoa.NewJobSummary(
			convertJobInfoToJobSummary(jobInfo),
			podSpec,
		)
		if err != nil {
			return nil, fmt.Errorf("new job summary: %s", err)
		}

		return s, nil
	}

	outputs, err := concurrency.Map(
		ctx,
		concurrency.MapperFunc(f),
		inputs,
		h.config.GetJobSummaryWorkers)
	if err != nil {
		return nil, auroraErrorf(err.Error())
	}

	summaries := []*api.JobSummary{}
	for _, o := range outputs {
		if o == nil {
			continue
		}
		summary := o.(*api.JobSummary)
		if summary == nil {
			continue
		}
		summaries = append(summaries, summary)
	}

	return &api.Result{
		JobSummaryResult: &api.JobSummaryResult{
			Summaries: summaries,
		},
	}, nil
}

// GetTasksWithoutConfigs is the same as getTasksStatus but without the TaskConfig.ExecutorConfig
// data set.
func (h *ServiceHandler) GetTasksWithoutConfigs(
	ctx context.Context,
	query *api.TaskQuery,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.getTasksWithoutConfigs(ctx, query)
	resp := newResponse(result, err, "getTasksWithoutConfigs")

	defer func() {
		h.metrics.
			Procedures[ProcedureGetTasksWithoutConfigs].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureGetTasksWithoutConfigs].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"query": query,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("GetTasksWithoutConfigs error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"query": query,
			},
			"result": result,
		}).Debug("GetTasksWithoutConfigs success")
	}()

	return resp, nil
}

func (h *ServiceHandler) getTasksWithoutConfigs(
	ctx context.Context,
	query *api.TaskQuery,
) (*api.Result, *auroraError) {

	var podStates []pod.PodState
	for s := range query.GetStatuses() {
		p, err := atop.NewPodState(s)
		if err != nil {
			return nil, auroraErrorf("new pod state: %s", err)
		}
		podStates = append(podStates, p)
	}

	jobIDs, err := h.getJobIDsFromTaskQuery(ctx, query)
	if err != nil {
		return nil, auroraErrorf("get job ids from task query: %s", err)
	}

	// concurrency.Map in nested setup leaks goroutines, thus using
	// a custom worker pool implementation.
	mapOutputs := make([]struct {
		ts  []*api.ScheduledTask
		err error
	}, len(jobIDs))
	mapWait := sync.WaitGroup{}
	for i, jobID := range jobIDs {
		mapWait.Add(1)
		go func(i int, j *peloton.JobID) {
			defer mapWait.Done()

			jobSummary, err := h.getJobInfoSummary(ctx, j)
			if err != nil {
				if yarpcerrors.IsNotFound(err) {
					return
				}
				mapOutputs[i].err = fmt.Errorf(
					"get job info for job id %q: %s",
					j.GetValue(), err)
				return
			}

			pods, err := h.queryPods(
				ctx,
				j,
				jobSummary.GetInstanceCount(),
			)
			if err != nil {
				mapOutputs[i].err = fmt.Errorf(
					"query pods for job id %q: %s",
					j.GetValue(), err)
				return
			}

			ts, err := h.getScheduledTasks(
				ctx,
				jobSummary,
				pods,
				&taskFilter{statuses: query.GetStatuses()},
			)
			if err != nil {
				mapOutputs[i].err = fmt.Errorf("get tasks without configs: %s", err)
				return
			}

			mapOutputs[i].ts = ts
			return
		}(i, jobID)
	}
	mapWait.Wait()

	tasks := []*api.ScheduledTask{}
	for _, o := range mapOutputs {
		if o.err != nil {
			return nil, auroraErrorf(o.err.Error())
		}
		if o.ts == nil {
			continue
		}
		tasks = append(tasks, o.ts...)
	}

	return &api.Result{
		ScheduleStatusResult: &api.ScheduleStatusResult{
			Tasks: tasks,
		},
	}, nil
}

type taskFilter struct {
	statuses map[api.ScheduleStatus]struct{}
}

// include returns true if s is allowed by the filter.
func (f *taskFilter) include(t *api.ScheduledTask) bool {
	if len(f.statuses) > 0 {
		if _, ok := f.statuses[t.GetStatus()]; !ok {
			return false
		}
	}
	return true
}

type getScheduledTaskInput struct {
	podName    *peloton.PodName
	podID      *peloton.PodID
	instanceID uint32

	// For current run
	jobSummary *stateless.JobSummary
	podSpec    *pod.PodSpec
}

// getPodRunsLimit calculates the number of pod runs getTasksWithoutConfigs
// endpoint will return based on config.
func getPodRunsLimit(
	podsNum uint32,
	podsMax uint32,
	podRunsDepth uint32,
) uint32 {
	podsNumTotal := util.Min(podsNum*podRunsDepth, podsMax)
	podRuns := uint32(math.Ceil(float64(podsNumTotal) / float64(podsNum)))
	return util.Max(podRuns, util.Min(podRunsDepth, minPodRunsDepth))
}

// getScheduledTasks generates a list of Aurora ScheduledTask in a worker
// pool.
func (h *ServiceHandler) getScheduledTasks(
	ctx context.Context,
	jobSummary *stateless.JobSummary,
	podInfos []*pod.PodInfo,
	filter *taskFilter,
) ([]*api.ScheduledTask, error) {
	jobID := jobSummary.GetJobId()

	podRunsDepth := getPodRunsLimit(
		uint32(len(podInfos)),
		uint32(h.config.GetTasksPodMax),
		uint32(h.config.PodRunsDepth),
	)

	var inputs []interface{}
	for _, p := range podInfos {
		podSpec := p.GetSpec()
		podID := p.GetStatus().GetPodId()
		podName := podSpec.GetPodName()

		runID, err := util.ParseRunID(podID.GetValue())
		if err != nil {
			return nil, fmt.Errorf("failed to parse pod id: %s", err)
		}

		_, instanceID, err := util.ParseTaskID(podName.GetValue())
		if err != nil {
			return nil, fmt.Errorf("failed to parse pod name: %s", err)
		}

		// when PodRunsDepth set to 1, query only current run pods, when set
		// to larger than 1, will query current plus previous run pods
		for i := uint64(0); i < uint64(podRunsDepth); i++ {
			newRunID := runID - i
			if newRunID == 0 {
				// No more previous run pods
				break
			}

			newPodID := &peloton.PodID{
				Value: util.CreateMesosTaskID(&v0peloton.JobID{
					Value: jobID.GetValue(),
				}, instanceID, newRunID).GetValue(),
			}

			taskInput := &getScheduledTaskInput{
				podName:    podName,
				instanceID: instanceID,
			}

			if i == 0 {
				// Attach for current run, leave podID to nil so that
				// current run will be queried
				taskInput.jobSummary = jobSummary
				taskInput.podSpec = podSpec
			} else {
				// Attach for previous run
				taskInput.podID = newPodID
			}

			inputs = append(inputs, taskInput)
		}
	}

	lock := &sync.Mutex{}
	jobInfoVersionMap := make(map[string]*stateless.JobInfo)

	getJobInfoForVersion := func(podVersion string) (*stateless.JobInfo, error) {
		lock.Lock()
		defer lock.Unlock()

		var jobInfo *stateless.JobInfo
		var err error

		jobInfo, ok := jobInfoVersionMap[podVersion]
		if !ok {
			jobInfo, err = h.getFullJobInfoByVersion(
				ctx,
				jobID,
				&peloton.EntityVersion{Value: podVersion},
			)
			if err != nil {
				return nil, fmt.Errorf("get job info by version: %s", err)
			}

			jobInfoVersionMap[podVersion] = jobInfo
		}

		return jobInfo, nil
	}

	f := func(ctx context.Context, input interface{}) (interface{}, error) {
		taskInput, ok := input.(*getScheduledTaskInput)
		if !ok {
			return nil, fmt.Errorf("failed to cast to get scheduled task input")
		}

		podName := taskInput.podName
		podID := taskInput.podID
		instanceID := taskInput.instanceID

		podEvents, err := h.getPodEvents(
			ctx,
			podName,
			podID,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"get pod events for pod %q with pod id %q: %s",
				podName.GetValue(), podID.GetValue(), err)
		}
		if len(podEvents) == 0 {
			return nil, nil
		}

		var t *api.ScheduledTask

		if taskInput.jobSummary != nil && taskInput.podSpec != nil {
			// For current pod run
			t, err = ptoa.NewScheduledTask(
				taskInput.jobSummary,
				taskInput.podSpec,
				podEvents,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"new scheduled task: %s", err)
			}
		} else {
			// For previous pod run
			podVersion := podEvents[0].GetVersion().GetValue()
			if len(podVersion) == 0 {
				return nil, fmt.Errorf(
					"cannot find pod version for pod: %s",
					podID.GetValue())
			}

			prevJobInfo, err := getJobInfoForVersion(podVersion)
			if err != nil {
				return nil, fmt.Errorf("get job info for version: %s", err)
			}

			prevJobSummary := convertJobInfoToJobSummary(prevJobInfo)
			prevPodSpec := getPodSpecForInstance(prevJobInfo.GetSpec(), instanceID)

			t, err = ptoa.NewScheduledTask(prevJobSummary, prevPodSpec, podEvents)
			if err != nil {
				return nil, fmt.Errorf(
					"new scheduled task: %s", err)
			}
		}

		if !filter.include(t) {
			return nil, nil
		}

		return t, nil
	}

	workers := h.config.getTasksWithoutConfigsWorkers(len(inputs))

	outputs, err := concurrency.Map(
		ctx,
		concurrency.MapperFunc(f),
		inputs,
		workers)
	if err != nil {
		return nil, err
	}

	var tasks []*api.ScheduledTask
	for _, o := range outputs {
		t := o.(*api.ScheduledTask)
		if t == nil {
			continue
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// GetConfigSummary fetches the configuration summary of active tasks for the specified job.
func (h *ServiceHandler) GetConfigSummary(
	ctx context.Context,
	job *api.JobKey) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.getConfigSummary(ctx, job)
	resp := newResponse(result, err, "getConfigSummary")

	defer func() {
		h.metrics.
			Procedures[ProcedureGetConfigSummary].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureGetConfigSummary].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"job": job,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("GetConfigSummary error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"job": job,
			},
			"result": result,
		}).Debug("GetConfigSummary success")
	}()

	return resp, nil
}

func (h *ServiceHandler) getConfigSummary(
	ctx context.Context,
	jobKey *api.JobKey,
) (*api.Result, *auroraError) {
	jobID, err := h.getJobID(
		ctx,
		jobKey)
	if err != nil {
		return nil, auroraErrorf("unable to get jobID from jobKey: %s", err)
	}

	jobSummary, err := h.getJobInfoSummary(ctx, jobID)
	if err != nil {
		return nil, auroraErrorf("unable to get jobSummary from jobID: %s", err)
	}

	podInfos, err := h.queryPods(
		ctx,
		jobID,
		jobSummary.GetInstanceCount())
	if err != nil {
		return nil, auroraErrorf("unable to query pods using jobID: %s", err)
	}

	configSummary, err := ptoa.NewConfigSummary(
		jobSummary,
		podInfos)
	if err != nil {
		return nil, auroraErrorf("unable to get config summary from podInfos: %s", err)
	}

	return &api.Result{
		ConfigSummaryResult: &api.ConfigSummaryResult{
			Summary: configSummary,
		},
	}, nil
}

// GetJobs fetches the status of jobs. ownerRole is optional, in which case all jobs are returned.
func (h *ServiceHandler) GetJobs(
	ctx context.Context,
	ownerRole *string,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.getJobs(ctx, ownerRole)
	resp := newResponse(result, err, "getJobs")

	defer func() {
		h.metrics.
			Procedures[ProcedureGetJobs].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureGetJobs].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"ownerRole": ownerRole,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("GetJobs error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"ownerRole": ownerRole,
			},
			"result": result,
		}).Debug("GetJobs success")
	}()

	return resp, nil
}

func (h *ServiceHandler) getJobs(
	ctx context.Context,
	ownerRole *string,
) (*api.Result, *auroraError) {
	var jobIDs []*peloton.JobID
	var err error

	if ownerRole != nil && *ownerRole != "" {
		jobIDs, err = h.getJobIDsFromRoleCache(ctx, *ownerRole)
	} else {
		jobIDs, err = h.queryJobIDs(ctx, "", "", "")
	}

	if err != nil {
		return nil, auroraErrorf("get job ids from role: %s", err)
	}

	var inputs []interface{}
	for _, j := range jobIDs {
		inputs = append(inputs, j)
	}

	f := func(ctx context.Context, input interface{}) (interface{}, error) {
		jobID := input.(*peloton.JobID)
		jobInfo, err := h.getJobInfo(ctx, jobID)
		if err != nil {
			if yarpcerrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("get job info for job id %q: %s",
				jobID.GetValue(), err)
		}

		// If the job is stopped, skip it from the response
		if jobInfo.GetSpec().GetInstanceCount() == 0 {
			return nil, nil
		}

		// In Aurora, JobConfiguration.TaskConfig
		// is generated using latest "active" task. Reference:
		// https://github.com/apache/aurora/blob/master/src/main/java/org/apache/aurora/scheduler/base/Tasks.java#L133
		// but use JobInfo.JobSpec.DefaultSpec here to simplify
		// the querying logic.
		// TODO(kevinxu): Need to match Aurora's behavior?
		// TODO(kevinxu): Need to inspect InstanceSpec as well?
		podSpec := jobInfo.GetSpec().GetDefaultSpec()

		c, err := ptoa.NewJobConfiguration(
			convertJobInfoToJobSummary(jobInfo),
			podSpec,
			true)
		if err != nil {
			return nil, fmt.Errorf("new job configuration: %s", err)
		}

		return c, nil
	}

	outputs, err := concurrency.Map(
		ctx,
		concurrency.MapperFunc(f),
		inputs,
		h.config.GetJobsWorkers)
	if err != nil {
		return nil, auroraErrorf(err.Error())
	}

	configs := []*api.JobConfiguration{}
	for _, o := range outputs {
		if o == nil {
			continue
		}

		config := o.(*api.JobConfiguration)
		if config == nil {
			continue
		}
		configs = append(configs, config)
	}

	return &api.Result{
		GetJobsResult: &api.GetJobsResult{
			Configs: configs,
		},
	}, nil
}

// GetJobUpdateSummaries gets job update summaries.
func (h *ServiceHandler) GetJobUpdateSummaries(
	ctx context.Context,
	query *api.JobUpdateQuery,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.getJobUpdateSummaries(ctx, query)
	resp := newResponse(result, err, "getJobUpdateSummaries")

	defer func() {
		h.metrics.
			Procedures[ProcedureGetJobUpdateSummaries].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureGetJobUpdateSummaries].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"query": query,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("GetJobUpdateSummaries error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"query": query,
			},
			"result": result,
		}).Debug("GetJobUpdateSummaries success")
	}()

	return resp, nil
}

func (h *ServiceHandler) getJobUpdateSummaries(
	ctx context.Context,
	query *api.JobUpdateQuery,
) (*api.Result, *auroraError) {

	details, err := h.queryJobUpdates(ctx, query, false /* includeInstanceEvents */)
	if err != nil {
		return nil, auroraErrorf("query job updates: %s", err)
	}
	summaries := []*api.JobUpdateSummary{}
	for _, d := range details {
		summaries = append(summaries, d.GetUpdate().GetSummary())
	}
	return &api.Result{
		GetJobUpdateSummariesResult: &api.GetJobUpdateSummariesResult{
			UpdateSummaries: summaries,
		},
	}, nil
}

// GetJobUpdateDetails gets job update details.
// jobUpdateKey is marked to be deprecated from Aurora, and not used Aggregator
// It will be ignored to get job update details
func (h *ServiceHandler) GetJobUpdateDetails(
	ctx context.Context,
	key *api.JobUpdateKey,
	query *api.JobUpdateQuery,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.getJobUpdateDetails(ctx, key, query)
	resp := newResponse(result, err, "getJobUpdateDetails")

	defer func() {
		h.metrics.
			Procedures[ProcedureGetJobUpdateDetails].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureGetJobUpdateDetails].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"key":   key,
					"query": query,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("GetJobUpdateDetails error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"key":   key,
				"query": query,
			},
			"result": result,
		}).Debug("GetJobUpdateDetails success")
	}()

	return resp, nil
}

func (h *ServiceHandler) getJobUpdateDetails(
	ctx context.Context,
	key *api.JobUpdateKey,
	query *api.JobUpdateQuery,
) (*api.Result, *auroraError) {

	if key.IsSetJob() {
		query.JobKey = key.GetJob()
	}
	details, err := h.queryJobUpdates(ctx, query, true /* includeInstanceEvents */)
	if err != nil {
		return nil, auroraErrorf("query job updates: %s", err)
	}
	if details == nil {
		details = []*api.JobUpdateDetails{}
	}
	return &api.Result{
		GetJobUpdateDetailsResult: &api.GetJobUpdateDetailsResult{
			DetailsList: details,
		},
	}, nil
}

// GetJobUpdateDiff gets the diff between client (desired) and server (current) job states.
// TaskConfig is not set in GetJobUpdateDiffResult, since caller is not using it
// and fetching previous podspec is expensive
func (h *ServiceHandler) GetJobUpdateDiff(
	ctx context.Context,
	request *api.JobUpdateRequest) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.getJobUpdateDiff(ctx, request)
	resp := newResponse(result, err, "getJobUpdateDiff")

	defer func() {
		h.metrics.
			Procedures[ProcedureGetJobUpdateDiff].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureGetJobUpdateDiff].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"request": request,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("GetJobUpdateDiff error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"request": request,
			},
			"result": result,
		}).Debug("GetJobUpdateDiff success")
	}()

	return resp, nil
}

func (h *ServiceHandler) getJobUpdateDiff(
	ctx context.Context,
	request *api.JobUpdateRequest,
) (*api.Result, *auroraError) {

	respoolID, err := h.respoolLoader.Load(
		ctx,
		label.IsGpuConfig(
			request.GetTaskConfig().GetMetadata(),
			request.GetTaskConfig().GetResources(),
		),
	)
	if err != nil {
		return nil, auroraErrorf("load respool: %s", err)
	}

	newJobResult := func() (*api.Result, *auroraError) {
		last := max(0, request.GetInstanceCount()-1)
		return &api.Result{
			GetJobUpdateDiffResult: &api.GetJobUpdateDiffResult{
				Add: []*api.ConfigGroup{{
					Instances: []*api.Range{{
						First: ptr.Int32(0),
						Last:  ptr.Int32(last),
					}},
				}},
			},
		}, nil
	}

	jobID, err := h.getJobID(ctx, request.GetTaskConfig().GetJob())
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			// Peloton returns errors for non-existent jobs in GetReplaceJobDiff,
			// so construct the diff manually in this case.
			return newJobResult()
		}
		return nil, auroraErrorf("get job id: %s", err)
	}

	jobSummary, err := h.getJobInfoSummary(ctx, jobID)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			// Peloton returns errors for non-existent jobs in GetReplaceJobDiff,
			// so construct the diff manually in this case.
			return newJobResult()
		}
		return nil, auroraErrorf("get job summary: %s", err)
	}

	jobSpec, err := atop.NewJobSpecFromJobUpdateRequest(
		request,
		respoolID,
		h.config.ThermosExecutor,
	)
	if err != nil {
		return nil, auroraErrorf("new job spec: %s", err)
	}

	resp, err := h.jobClient.GetReplaceJobDiff(
		ctx,
		&statelesssvc.GetReplaceJobDiffRequest{
			JobId:   jobID,
			Version: jobSummary.GetStatus().GetVersion(),
			Spec:    jobSpec,
		})
	if err != nil {
		return nil, auroraErrorf("get replace job diff: %s", err)
	}

	return &api.Result{GetJobUpdateDiffResult: &api.GetJobUpdateDiffResult{
		Add:       ptoa.NewConfigGroupWithoutTaskConfig(resp.GetInstancesAdded()),
		Update:    ptoa.NewConfigGroupWithoutTaskConfig(resp.GetInstancesUpdated()),
		Remove:    ptoa.NewConfigGroupWithoutTaskConfig(resp.GetInstancesRemoved()),
		Unchanged: ptoa.NewConfigGroupWithoutTaskConfig(resp.GetInstancesUnchanged()),
	}}, nil
}

// GetTierConfigs is a no-op. It is only used to determine liveness of the scheduler.
func (h *ServiceHandler) GetTierConfigs(
	ctx context.Context,
) (*api.Response, error) {

	startTime := time.Now()
	result := &api.Result{
		GetTierConfigResult: &api.GetTierConfigResult{
			DefaultTierName: ptr.String(common.Preemptible),
			Tiers: []*api.TierConfig{
				{
					Name: ptr.String(common.Revocable),
					Settings: map[string]string{
						common.Preemptible: "true",
						common.Revocable:   "true",
					},
				},
				{
					Name: ptr.String(common.Preferred),
					Settings: map[string]string{
						common.Preemptible: "false",
						common.Revocable:   "false",
					},
				},
				{
					Name: ptr.String(common.Preemptible),
					Settings: map[string]string{
						common.Preemptible: "true",
						common.Revocable:   "false",
					},
				},
			},
		},
	}
	resp := newResponse(result, nil)

	defer func() {
		h.metrics.
			Procedures[ProcedureGetTierConfigs].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureGetTierConfigs].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		log.WithFields(log.Fields{
			"result": result,
		}).Debug("GetTierConfigs success")
	}()

	return resp, nil
}

// KillTasks initiates a kill on tasks.
func (h *ServiceHandler) KillTasks(
	ctx context.Context,
	job *api.JobKey,
	instances map[int32]struct{},
	message *string,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.killTasks(ctx, job, instances, message)
	resp := newResponse(result, err, "killTasks")

	defer func() {
		h.metrics.
			Procedures[ProcedureKillTasks].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureKillTasks].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		var instancesArr []string
		for instanceID := range instances {
			instancesArr = append(instancesArr, string(instanceID))
		}
		instancesStr := strings.Join(instancesArr, ",")

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"job":       job,
					"instances": instancesStr,
					"message":   message,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("KillTasks error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"job":       job,
				"instances": instancesStr,
				"message":   message,
			},
		}).Info("KillTasks success")
	}()
	return resp, nil
}

func (h *ServiceHandler) killTasks(
	ctx context.Context,
	job *api.JobKey,
	instances map[int32]struct{},
	message *string,
) (*api.Result, *auroraError) {

	id, err := h.getJobID(ctx, job)
	if err != nil {
		return nil, auroraErrorf("get job id: %s", err)
	}
	summary, err := h.getJobInfoSummary(ctx, id)
	if err != nil {
		return nil, auroraErrorf("get job info summary: %s", err)
	}

	stopAll := false
	if uint32(len(instances)) == summary.GetInstanceCount() {
		// Sanity check to make sure we don't stop everything if instances
		// are out of bounds.
		low, high := instanceBounds(instances)
		if low == 0 && high == int32(len(instances)-1) {
			stopAll = true
		}
	}

	if instances == nil || len(instances) == 0 {
		// if instances is not passed in, assuming killing all tasks
		stopAll = true
	}

	if stopAll {
		// If all instances are specified, issue a single StopJob instead of
		// multiple StopPods for performance reasons.
		req := &statelesssvc.StopJobRequest{
			JobId:   id,
			Version: summary.GetStatus().GetVersion(),
		}
		if _, err := h.jobClient.StopJob(ctx, req); err != nil {
			return nil, auroraErrorf("stop job: %s", err)
		}
	} else {
		if err := h.stopPodsConcurrently(ctx, id, instances); err != nil {
			return nil, auroraErrorf("stop pods in parallel: %s", err)
		}
	}
	return dummyResult(), nil
}

func (h *ServiceHandler) stopPodsConcurrently(
	ctx context.Context,
	id *peloton.JobID,
	instances map[int32]struct{},
) error {

	var inputs []interface{}
	for i := range instances {
		inputs = append(inputs, i)
	}

	f := func(ctx context.Context, input interface{}) (interface{}, error) {
		instanceID := input.(int32)
		name := util.CreatePelotonTaskID(id.GetValue(), uint32(instanceID))
		req := &podsvc.StopPodRequest{
			PodName: &peloton.PodName{Value: name},
		}

		resp, err := h.podClient.StopPod(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("stop pod %d: %s", instanceID, err)
		}

		return resp, nil
	}

	_, err := concurrency.Map(
		ctx,
		concurrency.MapperFunc(f),
		inputs,
		h.config.StopPodWorkers)

	return err
}

// instanceBounds returns the lowest and highest instance id of
// instances. If instances is empty, returns -1.
func instanceBounds(instances map[int32]struct{}) (low, high int32) {
	if len(instances) == 0 {
		return -1, -1
	}
	for i := range instances {
		if i < low {
			low = i
		}
		if i > high {
			high = i
		}
	}
	return low, high
}

// StartJobUpdate starts update of the existing service job.
func (h *ServiceHandler) StartJobUpdate(
	ctx context.Context,
	request *api.JobUpdateRequest,
	message *string,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.startJobUpdate(ctx, request, message)
	resp := newResponse(result, err, "startJobUpdate")

	defer func() {
		updateService := request.GetTaskConfig().GetJob().GetRole()
		responseCode := resp.GetResponseCode()
		responseTime := time.Since(startTime)

		if len(updateService) > 0 {
			h.metrics.
				Procedures[ProcedureStartJobUpdate].
				ResponseCodes[responseCode].
				Scope.
				Tagged(map[string]string{
					TagService: updateService,
				}).
				Counter(MetricNameCalls).
				Inc(1)
			h.metrics.
				Procedures[ProcedureStartJobUpdate].
				ResponseCodes[responseCode].
				Scope.
				Tagged(map[string]string{
					TagService: updateService,
				}).
				Timer(MetricNameCallLatency).
				Record(responseTime)
		} else {
			h.metrics.
				Procedures[ProcedureStartJobUpdate].
				ResponseCodes[responseCode].
				Calls.
				Inc(1)
			h.metrics.
				Procedures[ProcedureStartJobUpdate].
				ResponseCodes[responseCode].
				CallLatency.
				Record(responseTime)
		}

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"request": request,
					"message": message,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("StartJobUpdate error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"message": message,
				"request": request, // TODO (varung): remove post PRR or as necessary
			},
			"update_id":        result.GetStartJobUpdateResult().GetKey().GetID(),
			"job_update_state": result.GetStartJobUpdateResult().GetUpdateSummary().GetState().String(),
		}).Info("StartJobUpdate success")
	}()

	return resp, nil
}

// PauseJobUpdate pauses the specified job update. Can be resumed by resumeUpdate call.
func (h *ServiceHandler) PauseJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.pauseJobUpdate(ctx, key, message)
	resp := newResponse(result, err, "pauseJobUpdate")

	defer func() {
		h.metrics.
			Procedures[ProcedurePauseJobUpdate].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedurePauseJobUpdate].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"key":     key,
					"message": message,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("PauseJobUpdate error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"job":       key.GetJob(),
				"update_id": key.GetID(),
				"message":   message,
			},
		}).Info("PauseJobUpdate success")
	}()

	return resp, nil
}

func (h *ServiceHandler) pauseJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Result, *auroraError) {

	id, err := h.getJobID(ctx, key.GetJob())
	if err != nil {
		return nil, auroraErrorf("get job id: %s", err)
	}
	v, aerr := h.matchJobUpdateID(ctx, id, key.GetID())
	if aerr != nil {
		return nil, aerr
	}
	req := &statelesssvc.PauseJobWorkflowRequest{
		JobId:   id,
		Version: v,
	}
	if _, err := h.jobClient.PauseJobWorkflow(ctx, req); err != nil {
		return nil, auroraErrorf("pause job workflow: %s", err)
	}
	return dummyResult(), nil
}

// ResumeJobUpdate resumes progress of a previously paused job update.
func (h *ServiceHandler) ResumeJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.resumeJobUpdate(ctx, key, message)
	resp := newResponse(result, err, "resumeJobUpdate")

	defer func() {
		h.metrics.
			Procedures[ProcedureResumeJobUpdate].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureResumeJobUpdate].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"key":     key,
					"message": message,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("ResumeJobUpdate error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"job":       key.GetJob(),
				"update_id": key.GetID(),
				"message":   message,
			},
		}).Info("ResumeJobUpdate success")
	}()

	return resp, nil
}

func (h *ServiceHandler) resumeJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Result, *auroraError) {

	id, err := h.getJobID(ctx, key.GetJob())
	if err != nil {
		return nil, auroraErrorf("get job id: %s", err)
	}

	v, aerr := h.matchJobUpdateID(ctx, id, key.GetID())
	if aerr != nil {
		return nil, aerr
	}

	req := &statelesssvc.ResumeJobWorkflowRequest{
		JobId:   id,
		Version: v,
	}
	if _, err := h.jobClient.ResumeJobWorkflow(ctx, req); err != nil {
		return nil, auroraErrorf("resume job workflow: %s", err)
	}

	return dummyResult(), nil
}

// AbortJobUpdate permanently aborts the job update. Does not remove the update history.
func (h *ServiceHandler) AbortJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.abortJobUpdate(ctx, key, message)
	resp := newResponse(result, err, "abortJobUpdate")

	defer func() {
		h.metrics.
			Procedures[ProcedureAbortJobUpdate].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureAbortJobUpdate].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"key":     key,
					"message": message,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("AbortJobUpdate error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"job":       key.GetJob(),
				"update_id": key.GetID(),
				"message":   message,
			},
		}).Info("AbortJobUpdate success")
	}()

	return resp, nil
}

func (h *ServiceHandler) abortJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Result, *auroraError) {

	id, err := h.getJobID(ctx, key.GetJob())
	if err != nil {
		return nil, auroraErrorf("get job id: %s", err)
	}

	v, aerr := h.matchJobUpdateID(ctx, id, key.GetID())
	if aerr != nil {
		return nil, aerr
	}

	req := &statelesssvc.AbortJobWorkflowRequest{
		JobId:   id,
		Version: v,
	}
	if _, err := h.jobClient.AbortJobWorkflow(ctx, req); err != nil {
		return nil, auroraErrorf("abort job workflow: %s", err)
	}

	return dummyResult(), nil
}

// RollbackJobUpdate rollbacks the specified active job update to the initial state.
func (h *ServiceHandler) RollbackJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.rollbackJobUpdate(ctx, key, message)
	resp := newResponse(result, err, "rollbackJobUpdate")

	defer func() {
		h.metrics.
			Procedures[ProcedureRollbackJobUpdate].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedureRollbackJobUpdate].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"key":     key,
					"message": message,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("RollbackJobUpdate error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"job":       key.GetJob(),
				"update_id": key.GetID(),
				"message":   message,
			},
		}).Info("RollbackJobUpdate success")
	}()

	return resp, nil
}

// _validRollbackStatuses enumerates the statuses which a job update must be in
// for rollback to be valid.
var _validRollbackStatuses = common.NewJobUpdateStatusSet(
	api.JobUpdateStatusRollingForward,
	api.JobUpdateStatusRollForwardPaused,
	api.JobUpdateStatusRollForwardAwaitingPulse,
)

func (h *ServiceHandler) rollbackJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
	message *string,
) (*api.Result, *auroraError) {

	id, err := h.getJobID(ctx, key.GetJob())
	if err != nil {
		return nil, auroraErrorf("get job id: %s", err)
	}

	j, jobSpec, w, err := h.getJobAndWorkflow(ctx, id)
	if err != nil {
		return nil, auroraErrorf("get job: %s", err)
	}

	d, err := opaquedata.Deserialize(w.GetOpaqueData())
	if err != nil {
		return nil, auroraErrorf("deserialize opaque data: %s", err)
	}

	if d.UpdateID != key.GetID() {
		return nil, auroraErrorf(
			"update id does not match current update").
			code(api.ResponseCodeInvalidRequest)
	}

	status, err := ptoa.NewJobUpdateStatus(w.GetStatus().GetState(), d)
	if err != nil {
		return nil, auroraErrorf("new job update status: %s", err)
	}
	if !_validRollbackStatuses.Has(status) {
		return nil, auroraErrorf(
			"invalid rollback: update must be in %s", _validRollbackStatuses).
			code(api.ResponseCodeInvalidRequest)
	}

	d.AppendUpdateAction(opaquedata.Rollback)
	od, err := d.Serialize()
	if err != nil {
		return nil, auroraErrorf("serialize opaque data: %s", err)
	}

	prevVersion := w.GetStatus().GetPrevVersion()
	prevConfigVersion, _, _, err := versionutil.ParseJobEntityVersion(prevVersion)
	if err != nil {
		return nil, auroraErrorf("parse previous job entity version: %s", err)
	}

	var prevSpec *stateless.JobSpec
	if prevConfigVersion == 0 {
		// First deployment, no previous version config, use current spec
		// so that we can still get a valid spec on read path
		prevSpec = proto.Clone(jobSpec).(*stateless.JobSpec)
		// Set instance count to 0, so the rollback would bring existing
		// instances down
		prevSpec.InstanceCount = 0
	} else {
		prevJob, err := h.getFullJobInfoByVersion(ctx, id, prevVersion)
		if err != nil {
			return nil, auroraErrorf("get previous job: %s", err)
		}

		prevSpec = prevJob.GetSpec()
	}

	updateSpec := w.GetUpdateSpec()

	// Never rollback a rollback.
	updateSpec.RollbackOnFailure = false

	updateSpec.StartPaused = status == api.JobUpdateStatusRollForwardAwaitingPulse

	req := &statelesssvc.ReplaceJobRequest{
		JobId:   id,
		Version: j.GetVersion(),
		Spec:    prevSpec,
		//Secrets: nil,
		UpdateSpec: updateSpec,
		OpaqueData: od,
	}
	if _, err := h.jobClient.ReplaceJob(ctx, req); err != nil {
		return nil, auroraErrorf("replace job: %s", err)
	}
	return dummyResult(), nil
}

// PulseJobUpdate allows progress of the job update in case blockIfNoPulsesAfterMs is specified in
// JobUpdateSettings. Unblocks progress if the update was previously blocked.
// Responds with ResponseCode.INVALID_REQUEST in case an unknown update key is specified.
func (h *ServiceHandler) PulseJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
) (*api.Response, error) {

	startTime := time.Now()
	result, err := h.pulseJobUpdate(ctx, key)
	resp := newResponse(result, err, "pulseJobUpdate")

	defer func() {
		h.metrics.
			Procedures[ProcedurePulseJobUpdate].
			ResponseCodes[resp.GetResponseCode()].
			Calls.Inc(1)

		h.metrics.
			Procedures[ProcedurePulseJobUpdate].
			ResponseCodes[resp.GetResponseCode()].
			CallLatency.Record(time.Since(startTime))

		if err != nil {
			log.WithFields(log.Fields{
				"params": log.Fields{
					"key": key,
				},
				"code":  err.responseCode,
				"error": err.msg,
			}).Error("PulseJobUpdate error")
			return
		}

		log.WithFields(log.Fields{
			"params": log.Fields{
				"job":       key.GetJob(),
				"update_id": key.GetID(),
			},
			"result": result.GetPulseJobUpdateResult().GetStatus().String(),
		}).Info("PulseJobUpdate success")
	}()

	return resp, nil
}

// _validPulseStatuses enumerates the statuses which a job update must be in
// for pulse to be valid.
var _validPulseStatuses = common.NewJobUpdateStatusSet(
	api.JobUpdateStatusRollForwardAwaitingPulse,
	api.JobUpdateStatusRollBackAwaitingPulse,
)

func (h *ServiceHandler) pulseJobUpdate(
	ctx context.Context,
	key *api.JobUpdateKey,
) (*api.Result, *auroraError) {

	id, err := h.getJobID(ctx, key.GetJob())
	if err != nil {
		aerr := auroraErrorf("get job id: %s", err)
		if yarpcerrors.IsNotFound(err) {
			// Unknown update.
			// TODO(codyg): We should support some form of update ID.
			aerr.code(api.ResponseCodeInvalidRequest)
		}
		return nil, aerr
	}

	j, _, w, err := h.getJobAndWorkflow(ctx, id)
	if err != nil {
		return nil, auroraErrorf("get job status: %s", err)
	}

	d, err := opaquedata.Deserialize(w.GetOpaqueData())
	if err != nil {
		return nil, auroraErrorf("deserialize opaque data: %s", err)
	}

	if d.UpdateID != key.GetID() {
		return nil, auroraErrorf("update id does not match current update").
			code(api.ResponseCodeInvalidRequest)
	}

	status, err := ptoa.NewJobUpdateStatus(w.GetStatus().GetState(), d)
	if err != nil {
		return nil, auroraErrorf("new job update status: %s", err)
	}

	// Only resume if we're in a valid status. Else, pulseJobUpdate is
	// a no-op.
	if _validPulseStatuses.Has(status) {
		d.AppendUpdateAction(opaquedata.Pulse)
		od, err := d.Serialize()
		if err != nil {
			return nil, auroraErrorf("serialize opaque data: %s", err)
		}

		req := &statelesssvc.ResumeJobWorkflowRequest{
			JobId:      id,
			Version:    j.GetVersion(),
			OpaqueData: od,
		}
		if _, err := h.jobClient.ResumeJobWorkflow(ctx, req); err != nil {
			return nil, auroraErrorf("resume job workflow: %s", err)
		}
	}

	return &api.Result{
		PulseJobUpdateResult: &api.PulseJobUpdateResult{
			Status: api.JobUpdatePulseStatusOk.Ptr(),
		},
	}, nil
}

// queryJobUpdates is an awkward helper which returns JobUpdateDetails which
// will include instance events if flag is set.
func (h *ServiceHandler) queryJobUpdates(
	ctx context.Context,
	query *api.JobUpdateQuery,
	includeInstanceEvents bool,
) ([]*api.JobUpdateDetails, error) {

	filter := &updateFilter{
		id:       query.GetKey().GetID(),
		statuses: query.GetUpdateStatuses(),
	}

	jobs, err := h.getJobCacheFromJobUpdateQuery(ctx, query)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get job summaries: %s", err)
	}

	var inputs []interface{}
	for _, j := range jobs {
		inputs = append(inputs, j)
	}

	f := func(ctx context.Context, input interface{}) (interface{}, error) {
		return h.getFilteredJobUpdateDetails(
			ctx, input.(*jobCache), filter, includeInstanceEvents)
	}

	outputs, err := concurrency.Map(
		ctx,
		concurrency.MapperFunc(f),
		inputs,
		h.config.GetJobUpdateWorkers)
	if err != nil {
		return nil, fmt.Errorf("build job update details: %s", err)
	}

	var results []*api.JobUpdateDetails
	for _, o := range outputs {
		for _, d := range o.([]*api.JobUpdateDetails) {
			results = append(results, d)
		}
	}

	return results, nil
}

type updateFilter struct {
	id       string
	statuses map[api.JobUpdateStatus]struct{}
}

// include returns true if s is allowed by the filter.
func (f *updateFilter) include(s *api.JobUpdateSummary) bool {
	if f.id != "" && f.id != s.GetKey().GetID() {
		return false
	}
	if len(f.statuses) > 0 {
		if _, ok := f.statuses[s.GetState().GetStatus()]; !ok {
			return false
		}
	}
	return true
}

// getFilteredJobUpdateDetails fetches updates for job and prunes them according to
// the filter.
func (h *ServiceHandler) getFilteredJobUpdateDetails(
	ctx context.Context,
	job *jobCache,
	filter *updateFilter,
	includeInstanceEvents bool,
) ([]*api.JobUpdateDetails, error) {
	k, err := ptoa.NewJobKey(job.Name)
	if err != nil {
		return nil, fmt.Errorf("new job key: %s", err)
	}

	workflows, err := h.listWorkflows(ctx, job.JobId, includeInstanceEvents)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list workflows: %s", err)
	}

	// Sort workflows by descending time order
	sort.Stable(sort.Reverse(ptoa.WorkflowsByMaxTS(workflows)))

	// Filter out any non-update workflows (i.e. restart), since they are not
	// valid updates generated by udeploy, and do not contain valid opaque data
	workflows = filterWorkflows(workflows, stateless.WorkflowType_WORKFLOW_TYPE_UPDATE)

	// Group updates by update id.
	detailsByID := make(map[string][]*api.JobUpdateDetails)
	var idOrder []string
	for i, w := range workflows {
		j := i + 1
		var prevWorkflow *stateless.WorkflowInfo
		if j < len(workflows) {
			prevWorkflow = workflows[j]
		}

		// TODO(kevinxu): skip no-op update to match aurora's behavior.
		// NOTE: If the update is no-op, aurora's behavior is to skip creating
		// the update.
		//if len(w.GetInstancesAdded())+len(w.GetInstancesUpdated())+
		//	len(w.GetInstancesRemoved()) == 0 {
		//	continue
		//}

		d, err := ptoa.NewJobUpdateDetails(k, prevWorkflow, w)
		if err != nil {
			return nil, fmt.Errorf("new job update details: %s", err)
		}
		id := d.GetUpdate().GetSummary().GetKey().GetID()
		detailsByID[id] = append(detailsByID[id], d)
		idOrder = append(idOrder, id)
	}

	var results []*api.JobUpdateDetails
	for _, id := range idOrder {
		details, ok := detailsByID[id]
		if !ok {
			continue
		}
		delete(detailsByID, id)

		var d *api.JobUpdateDetails
		switch len(details) {
		case 1:
			// NOTE: It is possible that this single update is actually a
			// rollback whose original update has already been pruned from
			// Peloton. This is *probably* fine, since the consumer of this API
			// usually just cares if an update has been rolled back.
			d = details[0]
		case 2:
			// If two updates have the same id, it means one was an update and
			// the other was a rollback.
			d = ptoa.JoinRollbackJobUpdateDetails(details[0], details[1])
		default:
			// Nothing to do here but make noise and ignore the update.
			log.WithFields(log.Fields{
				"job_id":  job.JobId,
				"details": details,
			}).Error("Invariant violation: expected exactly 1 or 2 updates with same update id")
			continue
		}
		if filter.include(d.GetUpdate().GetSummary()) {
			results = append(results, d)
		}
	}

	return results, nil
}

// getJobSummariesFromJobUpdateQuery queries peloton jobs based on
// Aurora's JobUpdateQuery.
//
// Returns a yarpc NOT_FOUND error if no jobs match the query.
func (h *ServiceHandler) getJobCacheFromJobUpdateQuery(
	ctx context.Context,
	q *api.JobUpdateQuery,
) ([]*jobCache, error) {

	if q.IsSetKey() {
		return h.getJobCacheFromJobKey(ctx, q.GetKey().GetJob())
	}
	if q.IsSetJobKey() {
		return h.getJobCacheFromJobKey(ctx, q.GetJobKey())
	}

	jobmgrJobCaches, err := h.queryJobCache(ctx, q.GetRole(), "", "")
	if err != nil {
		return nil, err
	}

	var jobCaches []*jobCache
	for _, c := range jobmgrJobCaches {
		jobCaches = append(jobCaches, &jobCache{
			JobId: c.JobId,
			Name:  c.Name,
		})
	}
	return jobCaches, nil
}

// getJobID maps k to a job id.
//
// Note: Since we do not delete job name to id mapping when a job
// is deleted, we cannot rely on not-found error to determine the
// existence of a job.
//
// TODO: To be deprecated in favor of getJobCacheFromJobKey.
// Aggregator expects job key environment to be set in response of
// GetJobUpdateDetails to filter by deployment_id. On filtering via job key
// role, original peloton job name is not known to set job key environment.
func (h *ServiceHandler) getJobID(
	ctx context.Context,
	k *api.JobKey,
) (*peloton.JobID, error) {
	req := &statelesssvc.GetJobIDFromJobNameRequest{
		JobName: atop.NewJobName(k),
	}
	resp, err := h.jobClient.GetJobIDFromJobName(ctx, req)
	if err != nil {
		return nil, err
	}
	// results are sorted chronologically, return the latest one
	return resp.GetJobId()[0], nil
}

// queryJobIDs takes optional job key components and returns the Peloton job ids
// which match the set parameters. E.g. queryJobIDs("myservice", "", "") will return
// job ids which match role=myservice.
func (h *ServiceHandler) queryJobIDs(
	ctx context.Context,
	role, env, name string,
) ([]*peloton.JobID, error) {

	if role != "" && env != "" && name != "" {
		// All job key components set, just use a job key query directly.
		id, err := h.getJobID(ctx, &api.JobKey{
			Role:        ptr.String(role),
			Environment: ptr.String(env),
			Name:        ptr.String(name),
		})
		if err != nil {
			return nil, err
		}
		return []*peloton.JobID{id}, nil
	}

	jobCache, err := h.queryJobCache(ctx, role, env, name)
	if err != nil {
		return nil, err
	}

	jobIDs := make([]*peloton.JobID, 0, len(jobCache))
	for _, cache := range jobCache {
		jobIDs = append(jobIDs, cache.GetJobId())
	}
	return jobIDs, nil
}

// getJobCacheFromJobKey returns jobCache struct based on Aurora JobKey
// passed in.
func (h *ServiceHandler) getJobCacheFromJobKey(
	ctx context.Context,
	k *api.JobKey,
) ([]*jobCache, error) {
	req := &statelesssvc.GetJobIDFromJobNameRequest{
		JobName: atop.NewJobName(k),
	}

	resp, err := h.jobClient.GetJobIDFromJobName(ctx, req)
	if err != nil {
		return nil, err
	}

	// results are sorted chronologically, return the latest one
	return []*jobCache{
		{
			JobId: resp.GetJobId()[0],
			Name:  atop.NewJobName(k),
		},
	}, nil
}

// queryJobCache calls jobmgr's private QueryJobCache API, passes the querying
// labels for role, env and name parameters, and returns a list of JobCache
// objects.
func (h *ServiceHandler) queryJobCache(
	ctx context.Context,
	role, env, name string,
) ([]*jobmgrsvc.QueryJobCacheResponse_JobCache, error) {
	labels := append(
		label.BuildPartialAuroraJobKeyLabels(role, env, name),
		common.BridgeJobLabel,
	)
	req := &jobmgrsvc.QueryJobCacheRequest{
		Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
			Labels: labels,
		},
	}
	resp, err := h.jobmgrClient.QueryJobCache(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetResult(), nil
}

// getJobIDsFromTaskQuery queries peloton job ids based on aurora TaskQuery.
// Note that it will not throw error when no job is found. The current
// behavior for querying:
// 1. If TaskQuery.JobKeys is present, the job keys there to query job ids
// 2. Otherwise use TaskQuery.Role, TaskQuery.Environment and
//    TaskQuery.JobName to construct a job key (those 3 fields may not be
//    all present), and use it to query job ids.
//
// Note: Due to getJobID() may return invalid job ids, e.g. job ids that
// already deleted, be sure to check whether the error is "not-found" after
// querying using the job id.
func (h *ServiceHandler) getJobIDsFromTaskQuery(
	ctx context.Context,
	query *api.TaskQuery,
) ([]*peloton.JobID, error) {
	if query == nil {
		return nil, errors.New("task query is nil")
	}

	// use job_keys to query if present
	if query.IsSetJobKeys() {
		var ids []*peloton.JobID
		for _, jobKey := range query.GetJobKeys() {
			id, err := h.getJobID(ctx, jobKey)
			if err != nil {
				if yarpcerrors.IsNotFound(err) {
					continue
				}
				return nil, errors.Wrapf(err, "get job id for %q", jobKey)
			}
			ids = append(ids, id)
		}
		return ids, nil
	}

	ids, err := h.queryJobIDs(
		ctx, query.GetRole(), query.GetEnvironment(), query.GetJobName())
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			// ignore not found error and return empty job ids
			return nil, nil
		}
		return nil, errors.Wrapf(err, "get job ids")
	}
	return ids, nil
}

// getJobIDsFromRoleCache queries peloton job ids based on aurora JobKey role.
// It will first look at the internal job id cache first,
func (h *ServiceHandler) getJobIDsFromRoleCache(
	ctx context.Context,
	role string,
) ([]*peloton.JobID, error) {
	if ids := h.jobIdCache.GetJobIDs(role); len(ids) > 0 {
		return ids, nil
	}

	jobCache, err := h.queryJobCache(ctx, role, "", "")
	if err != nil {
		return nil, err
	}

	h.jobIdCache.PopulateFromJobCache(role, jobCache)

	jobIDs := make([]*peloton.JobID, 0, len(jobCache))
	for _, cache := range jobCache {
		jobIDs = append(jobIDs, cache.GetJobId())
	}
	return jobIDs, nil
}

// matchJobUpdateID matches a jobID workflow against updateID. Returns the entity
// version the workflow is moving towards. If the current workflow does not
// match updateID, returns an INVALID_REQUEST Aurora error.
func (h *ServiceHandler) matchJobUpdateID(
	ctx context.Context,
	jobID *peloton.JobID,
	updateID string,
) (*peloton.EntityVersion, *auroraError) {

	j, _, w, err := h.getJobAndWorkflow(ctx, jobID)
	if err != nil {
		return nil, auroraErrorf("get job status: %s", err)
	}
	d, err := opaquedata.Deserialize(w.GetOpaqueData())
	if err != nil {
		return nil, auroraErrorf("deserialize opaque data: %s", err)
	}
	if d.UpdateID != updateID {
		return nil, auroraErrorf("update id does not match current update").
			code(api.ResponseCodeInvalidRequest)
	}
	return j.GetVersion(), nil
}

// getJobInfo calls jobmgr to get JobInfo based on JobID.
func (h *ServiceHandler) getJobInfo(
	ctx context.Context,
	jobID *peloton.JobID,
) (*stateless.JobInfo, error) {
	req := &statelesssvc.GetJobRequest{
		JobId:       jobID,
		SummaryOnly: false,
	}
	resp, err := h.jobClient.GetJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetJobInfo(), nil
}

func (h *ServiceHandler) getFullJobInfoByVersion(
	ctx context.Context,
	jobID *peloton.JobID,
	v *peloton.EntityVersion,
) (*stateless.JobInfo, error) {
	req := &statelesssvc.GetJobRequest{
		JobId:   jobID,
		Version: v,
	}
	resp, err := h.jobClient.GetJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetJobInfo(), nil
}

// getJobInfoSummary calls jobmgr to get JobSummary based on JobID.
func (h *ServiceHandler) getJobInfoSummary(
	ctx context.Context,
	jobID *peloton.JobID,
) (*stateless.JobSummary, error) {
	req := &statelesssvc.GetJobRequest{
		JobId:       jobID,
		SummaryOnly: true,
	}
	resp, err := h.jobClient.GetJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetSummary(), nil
}

func (h *ServiceHandler) getJobAndWorkflow(
	ctx context.Context,
	id *peloton.JobID,
) (*stateless.JobStatus, *stateless.JobSpec, *stateless.WorkflowInfo, error) {

	resp, err := h.jobClient.GetJob(ctx, &statelesssvc.GetJobRequest{JobId: id})
	if err != nil {
		return nil, nil, nil, err
	}
	return resp.GetJobInfo().GetStatus(), resp.GetJobInfo().GetSpec(), resp.GetWorkflowInfo(), nil
}

// queryPods calls jobmgr to query a list of PodInfo based on input JobID.
func (h *ServiceHandler) queryPods(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceCount uint32,
) ([]*pod.PodInfo, error) {

	var inputs []interface{}
	for i := uint32(0); i < instanceCount; i++ {
		inputs = append(inputs, fmt.Sprintf("%s-%d", jobID.GetValue(), i))
	}

	workers := h.config.getTasksWithoutConfigsWorkers(len(inputs))

	f := func(ctx context.Context, input interface{}) (interface{}, error) {
		podName := input.(string)
		req := &podsvc.GetPodRequest{
			PodName: &peloton.PodName{
				Value: podName,
			},
			StatusOnly: false,
			Limit:      1,
		}

		resp, err := h.podClient.GetPod(ctx, req)
		if err != nil {
			// If TaskRuntime does not exist, return nil PodInfo
			if yarpcerrors.IsNotFound(err) {
				return nil, nil
			}

			return nil, err
		}

		return resp.GetCurrent(), nil
	}

	outputs, err := concurrency.Map(
		ctx,
		concurrency.MapperFunc(f),
		inputs,
		workers)
	if err != nil {
		return nil, err
	}

	podInfos := []*pod.PodInfo{}
	for _, o := range outputs {
		if o == nil {
			continue
		}
		podInfo := o.(*pod.PodInfo)
		if podInfo == nil {
			continue
		}
		podInfos = append(podInfos, podInfo)
	}

	return podInfos, nil
}

// getPodEvents calls jobmgr to get a list of PodEvent based on PodName.
func (h *ServiceHandler) getPodEvents(
	ctx context.Context,
	podName *peloton.PodName,
	podID *peloton.PodID,
) ([]*pod.PodEvent, error) {
	req := &podsvc.GetPodEventsRequest{
		PodName: podName,
		PodId:   podID,
	}
	resp, err := h.podClient.GetPodEvents(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetEvents(), nil
}

// listWorkflows lists recent workflows for jobID.
//
// Returns yarpc NOT_FOUND error if no workflows found.
func (h *ServiceHandler) listWorkflows(
	ctx context.Context,
	jobID *peloton.JobID,
	includeInstanceEvents bool,
) ([]*stateless.WorkflowInfo, error) {

	req := &statelesssvc.ListJobWorkflowsRequest{
		JobId:               jobID,
		InstanceEvents:      includeInstanceEvents,
		UpdatesLimit:        h.config.UpdatesLimit,
		InstanceEventsLimit: h.config.InstanceEventsLimit,
	}
	resp, err := h.jobClient.ListJobWorkflows(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetWorkflowInfos(), nil
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

// filterWorkflows returns a new slice of WorkflowInfo filtered by WorkflowType
func filterWorkflows(
	ws []*stateless.WorkflowInfo,
	wt stateless.WorkflowType,
) []*stateless.WorkflowInfo {
	wsf := make([]*stateless.WorkflowInfo, 0)
	for _, w := range ws {
		if w.GetStatus().GetType() != wt {
			continue
		}

		wsf = append(wsf, w)
	}
	return wsf
}

// dummyResult returns a dummy result since YARPC won't allow
// an empty union
func dummyResult() *api.Result {
	return &api.Result{
		GetTierConfigResult: &api.GetTierConfigResult{},
	}
}

// Converts job info to job summary
func convertJobInfoToJobSummary(
	jobInfo *stateless.JobInfo,
) *stateless.JobSummary {

	return &stateless.JobSummary{
		JobId:         jobInfo.GetJobId(),
		Name:          jobInfo.GetSpec().GetName(),
		Owner:         jobInfo.GetSpec().GetOwner(),
		InstanceCount: jobInfo.GetSpec().GetInstanceCount(),
		Sla:           jobInfo.GetSpec().GetSla(),
		Labels:        jobInfo.GetSpec().GetLabels(),
		RespoolId:     jobInfo.GetSpec().GetRespoolId(),
		Status:        jobInfo.GetStatus(),
	}
}
