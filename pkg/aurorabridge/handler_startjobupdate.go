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
	"io"
	"sync"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"
	"github.com/uber/peloton/pkg/common/concurrency"
	"github.com/uber/peloton/pkg/common/taskconfig"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.uber.org/thriftrw/ptr"
	"go.uber.org/yarpc/yarpcerrors"
)

// startJobUpdate is handler implementation for StartJobUpdate endpoint.
func (h *ServiceHandler) startJobUpdate(
	ctx context.Context,
	request *api.JobUpdateRequest,
	message *string,
) (*api.Result, *auroraError) {

	respoolID, err := h.respoolLoader.Load(ctx)
	if err != nil {
		return nil, auroraErrorf("load respool: %s", err)
	}

	jobKey := request.GetTaskConfig().GetJob()

	jobSpec, err := atop.NewJobSpecFromJobUpdateRequest(
		request,
		respoolID,
		h.config.ThermosExecutor,
	)
	if err != nil {
		return nil, auroraErrorf("new job spec: %s", err)
	}

	d := opaquedata.NewDataFromJobUpdateRequest(request, message)
	od, err := d.Serialize()
	if err != nil {
		return nil, auroraErrorf("serialize opaque data: %s", err)
	}

	createReq := &statelesssvc.CreateJobRequest{
		Spec:       jobSpec,
		CreateSpec: atop.NewCreateSpec(request.GetSettings()),
		OpaqueData: od,
	}

	updateResult := &api.Result{
		StartJobUpdateResult: &api.StartJobUpdateResult{
			Key: &api.JobUpdateKey{
				Job: jobKey,
				ID:  ptr.String(d.UpdateID),
			},
			UpdateSummary: nil, // TODO(codyg): Should we set this?
		},
	}

	// Attempt to query job id from job_name_to_id table
	id, err := h.getJobID(ctx, jobKey)
	if err != nil {
		if !yarpcerrors.IsNotFound(err) {
			return nil, auroraErrorf("get job id: %s", err)
		}

		// Job does not exist, create the job.
		if aerr := h.createJob(ctx, createReq); aerr != nil {
			return nil, aerr
		}

		return updateResult, nil
	}

	// Job exists in job_name_to_id table
	v, err := h.getCurrentJobVersion(ctx, id)
	if err != nil {
		if !yarpcerrors.IsNotFound(err) {
			return nil, auroraErrorf("get current job version: %s", err)
		}

		// Job was present in job_name_to_id table, but did not exist,
		// create the job.
		if aerr := h.createJob(ctx, createReq); aerr != nil {
			return nil, aerr
		}

		return updateResult, nil
	}

	// Job exists in job_name_to_id table and the job id is present,
	// update the job.
	updateJobSpec, err := h.createJobSpecForUpdate(ctx, request, id, jobSpec)
	if err != nil {
		return nil, auroraErrorf("create job spec for update: %s", err)
	}

	replaceReq := &statelesssvc.ReplaceJobRequest{
		JobId:      id,
		Spec:       updateJobSpec,
		UpdateSpec: atop.NewUpdateSpec(request.GetSettings()),
		Version:    v,
		OpaqueData: od,
	}
	if aerr := h.replaceJob(ctx, replaceReq); aerr != nil {
		return nil, aerr
	}

	return updateResult, nil
}

// createJobSpecForUpdate generates JobSpec which supports pinned instances.
func (h *ServiceHandler) createJobSpecForUpdate(
	ctx context.Context,
	req *api.JobUpdateRequest,
	jobID *peloton.JobID,
	jobSpec *stateless.JobSpec,
) (*stateless.JobSpec, error) {
	podStates, err := h.getCurrentPods(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("get current job: %s", err)
	}

	updateInstances := make(map[uint32]struct{})
	for _, r := range req.GetSettings().GetUpdateOnlyTheseInstances() {
		for i := uint32(r.GetFirst()); i <= uint32(r.GetLast()); i++ {
			updateInstances[i] = struct{}{}
		}
	}

	// TODO(kevinxu): Implement hack label case
	if len(updateInstances) == 0 {
		return jobSpec, nil
	}

	// TODO(kevinxu): Implement optimization logic so that we don't have
	// to attach pod spec for every instance.
	instances := uint32(req.GetInstanceCount())
	newJobSpec := proto.Clone(jobSpec).(*stateless.JobSpec)
	newPodSpec := newJobSpec.GetDefaultSpec()
	newJobSpec.DefaultSpec = nil
	newJobSpec.InstanceSpec = make(map[uint32]*pod.PodSpec)

	for i := uint32(0); i < instances; i++ {
		_, isCurrentInstance := podStates[i]
		_, isUpdateInstance := updateInstances[i]

		// Added instance, attach new pod spec
		if !isCurrentInstance {
			newJobSpec.InstanceSpec[i] = newPodSpec
			continue
		}

		curPodSpec := podStates[i].podSpec

		// Should not update instance, re-attach current pod spec
		if !isUpdateInstance {
			newJobSpec.InstanceSpec[i] = curPodSpec
			continue
		}

		newJobSpec.InstanceSpec[i] = newPodSpec
	}

	return newJobSpec, nil
}

// createJob calls CreateJob API using the input CreateJobRequest.
func (h *ServiceHandler) createJob(
	ctx context.Context,
	req *statelesssvc.CreateJobRequest,
) *auroraError {
	if _, err := h.jobClient.CreateJob(ctx, req); err != nil {
		if yarpcerrors.IsAlreadyExists(err) {
			return auroraErrorf(
				"create job: %s", err).
				code(api.ResponseCodeInvalidRequest)
		}
		return auroraErrorf("create job: %s", err)
	}
	return nil
}

// replaceJob calls ReplaceJob API using the input ReplaceJobRequest.
func (h *ServiceHandler) replaceJob(
	ctx context.Context,
	req *statelesssvc.ReplaceJobRequest,
) *auroraError {
	if _, err := h.jobClient.ReplaceJob(ctx, req); err != nil {
		if yarpcerrors.IsAborted(err) {
			// Upgrade conflict.
			return auroraErrorf(
				"replace job: %s", err).
				code(api.ResponseCodeInvalidRequest)
		}
		return auroraErrorf("replace job: %s", err)
	}
	return nil
}

// podStateSpec is a private struct used to hold PodState and resolved
// PodSpec by merging default spec and instance spec for a particular
// instance, it will be returned as a map with instance id being the key
// by getCurrentPods() method.
type podStateSpec struct {
	state   pod.PodState
	podSpec *pod.PodSpec
}

// getCurrentPods returns a map of instance ids to podStateSpec, which
// contains corresponding instance PodState and PodSpec. PodState is
// retrieved by calling ListPods API, and PodSpec is generated using
// Pod entity version (from ListPods API), and then calling GetJob API.
func (h *ServiceHandler) getCurrentPods(
	ctx context.Context,
	id *peloton.JobID,
) (map[uint32]*podStateSpec, error) {
	pods, err := h.listPods(ctx, id)
	if err != nil {
		return nil, err
	}

	podStates := make(map[uint32]*podStateSpec)
	podEntityVersion := make(map[uint32]string)
	entityVersionJobSpec := make(map[string]*stateless.JobSpec)

	// Use result from ListPods to populate pod state and current entity
	// version for each pod instance. Pods with same entity version will
	// be grouped together when calling GetJob to get the corresponding
	// PodSpec. It's implemented this way over using GetPod api calls to
	// avoid expensive db reads when dealing with jobs with large instance
	// count.
	for _, p := range pods {
		_, instanceID, err := util.ParseTaskID(p.GetPodName().GetValue())
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse pod name")
		}

		version := p.GetStatus().GetVersion().GetValue()
		if len(version) == 0 {
			return nil, fmt.Errorf(
				"invalid pod entity version for pod: %s",
				p.GetPodName(),
			)
		}

		podStates[instanceID] = &podStateSpec{state: p.GetStatus().GetState()}
		podEntityVersion[instanceID] = version
		entityVersionJobSpec[version] = nil
	}

	// Query JobSpec using grouped pod entity versions
	var inputs []interface{}
	for version := range entityVersionJobSpec {
		inputs = append(inputs, version)
	}

	versionLock := &sync.Mutex{}

	f := func(ctx context.Context, input interface{}) (interface{}, error) {
		version, ok := input.(string)
		if !ok {
			return nil, fmt.Errorf("failed to cast input to version string")
		}

		jobInfo, err := h.getFullJobInfoByVersion(
			ctx, id, &peloton.EntityVersion{Value: version})

		if err != nil {
			return nil, errors.Wrapf(err,
				"failed to get job for (%s, %s)",
				id.GetValue(), version)
		}

		versionLock.Lock()
		defer versionLock.Unlock()
		entityVersionJobSpec[version] = jobInfo.GetSpec()

		return nil, nil
	}

	_, err = concurrency.Map(
		ctx,
		concurrency.MapperFunc(f),
		inputs,
		h.config.GetCurrentPodsWorkers)
	if err != nil {
		return nil, err
	}

	// To get the currently running PodSpec for the instance, we need to
	// merge default spec and instance spec from the job spec retrieved
	// using the pod entity version.
	for instanceID, version := range podEntityVersion {
		podStates[instanceID].podSpec = getPodSpecForInstance(
			entityVersionJobSpec[version], instanceID,
		)
	}

	return podStates, nil
}

// getCurrentJobVersion calls GetJob API to query JobSummary based on
// input JobID, returns current job entity version.
func (h *ServiceHandler) getCurrentJobVersion(
	ctx context.Context,
	id *peloton.JobID,
) (*peloton.EntityVersion, error) {
	summary, err := h.getJobInfoSummary(ctx, id)
	if err != nil {
		return nil, err
	}
	return summary.GetStatus().GetVersion(), nil
}

// listPods calls ListPods stream API, waits for stream to end and returns
// a list of PodSummary.
func (h *ServiceHandler) listPods(
	ctx context.Context,
	id *peloton.JobID,
) ([]*pod.PodSummary, error) {
	// TODO(kevinxu): set request timeout
	req := &statelesssvc.ListPodsRequest{
		JobId: id,
	}
	stream, err := h.jobClient.ListPods(ctx, req)
	if err != nil {
		return nil, err
	}

	pods := make([]*pod.PodSummary, 0)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return pods, nil
		}
		if err != nil {
			return nil, err
		}
		pods = append(pods, resp.GetPods()...)
	}
}

// getPodSpecForInstance returns corresponding PodSpec for the particular
// instance by merge DefaultSpec and InstanceSpec, based on input JobSpec.
func getPodSpecForInstance(
	spec *stateless.JobSpec,
	instanceID uint32,
) *pod.PodSpec {
	return taskconfig.MergePodSpec(
		spec.GetDefaultSpec(),
		spec.GetInstanceSpec()[instanceID],
	)
}
