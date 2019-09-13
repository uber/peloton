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

package cli

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	v1alphapod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	v1alphaquery "github.com/uber/peloton/.gen/peloton/api/v1alpha/query"
	v1alpharespool "github.com/uber/peloton/.gen/peloton/api/v1alpha/respool"
	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"

	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"

	"github.com/golang/protobuf/ptypes"
	"go.uber.org/yarpc/yarpcerrors"
	yaml "gopkg.in/yaml.v2"
)

const (
	statelessJobSummaryFormatHeader = "ID\tName\tOwner\tState\tCreation Time\tTotal\t" +
		"Running\tSucceeded\tFailed\tKilled\t" +
		"Workflow Status\tCompleted\tFailed\tCurrent\n"
	statelessSummaryFormatBody = "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t\n"

	statelessUpdateEventsFormatHeader = "Type\tTimestamp\tState\t\n"
	statelessUpdateEventsFormatBody   = "%s\t%s\t%s\t\n"

	workflowEventsV1AlphaFormatHeader = "Workflow State\tWorkflow Type\tTimestamp\n"
	workflowEventsV1AlphaFormatBody   = "%s\t%s\t%s\n"

	queryPodsFormatHeader = "Pod ID\tName\tState\tContainer Name\tContainer State\tHealthy\tStart Time\tRun Time\t" +
		"Host\tMessage\tReason\tTermination Status\t\n"
	queryPodsFormatBody = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n"

	podListFormatHeader = "Name\tPod ID\tState\tHealthy\tStart Time\t" +
		"Host\tMessage\tReason\t\n"
	podListFormatBody = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n"
)

// StatelessGetCacheAction get cache of stateless job
func (c *Client) StatelessGetCacheAction(jobID string) error {
	resp, err := c.jobmgrClient.GetJobCache(
		c.ctx,
		&jobmgrsvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: jobID},
		})
	if err != nil {
		return err
	}

	out, err := marshallResponse(defaultResponseFormat, resp)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", string(out))

	return nil
}

// StatelessRefreshAction refreshes a job
func (c *Client) StatelessRefreshAction(jobID string) error {
	resp, err := c.jobmgrClient.RefreshJob(
		c.ctx,
		&jobmgrsvc.RefreshJobRequest{
			JobId: &v1alphapeloton.JobID{Value: jobID},
		})
	if err != nil {
		return err
	}

	out, err := marshallResponse(defaultResponseFormat, resp)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", string(out))

	return nil
}

// StatelessWorkflowPauseAction pauses a workflow
func (c *Client) StatelessWorkflowPauseAction(
	jobID string,
	entityVersion string,
	opaqueData string,
) error {
	var opaque *v1alphapeloton.OpaqueData
	if len(opaqueData) > 0 {
		opaque = &v1alphapeloton.OpaqueData{Data: opaqueData}
	}

	resp, err := c.statelessClient.PauseJobWorkflow(
		c.ctx,
		&statelesssvc.PauseJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: jobID},
			Version:    &v1alphapeloton.EntityVersion{Value: entityVersion},
			OpaqueData: opaque,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Workflow paused. New EntityVersion: %s\n", resp.GetVersion().GetValue())

	return nil
}

// StatelessWorkflowResumeAction resumes a workflow
func (c *Client) StatelessWorkflowResumeAction(
	jobID string,
	entityVersion string,
	opaqueData string,
) error {
	var opaque *v1alphapeloton.OpaqueData
	if len(opaqueData) > 0 {
		opaque = &v1alphapeloton.OpaqueData{Data: opaqueData}
	}

	resp, err := c.statelessClient.ResumeJobWorkflow(
		c.ctx,
		&statelesssvc.ResumeJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: jobID},
			Version:    &v1alphapeloton.EntityVersion{Value: entityVersion},
			OpaqueData: opaque,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Workflow resumed. New EntityVersion: %s\n", resp.GetVersion().GetValue())

	return nil
}

// StatelessWorkflowAbortAction aborts a workflow
func (c *Client) StatelessWorkflowAbortAction(
	jobID string,
	entityVersion string,
	opaqueData string,
) error {
	var opaque *v1alphapeloton.OpaqueData
	if len(opaqueData) > 0 {
		opaque = &v1alphapeloton.OpaqueData{Data: opaqueData}
	}

	resp, err := c.statelessClient.AbortJobWorkflow(
		c.ctx,
		&statelesssvc.AbortJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: jobID},
			Version:    &v1alphapeloton.EntityVersion{Value: entityVersion},
			OpaqueData: opaque,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Workflow aborted. New EntityVersion: %s\n", resp.GetVersion().GetValue())

	return nil
}

// StatelessStopJobAction stops a job
func (c *Client) StatelessStopJobAction(jobID string, entityVersion string) error {
	resp, err := c.statelessClient.StopJob(
		c.ctx,
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: jobID},
			Version: &v1alphapeloton.EntityVersion{Value: entityVersion},
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Job stopped. New EntityVersion: %s\n", resp.GetVersion().GetValue())

	return nil
}

// StatelessQueryAction queries a job given the spec
func (c *Client) StatelessQueryAction(
	labels string,
	respoolPath string,
	keywords string,
	states string,
	owner string,
	name string,
	days uint32,
	limit uint32,
	maxLimit uint32,
	offset uint32,
	sortBy string,
	sortOrder string) error {
	pelotonLabels, err := parseLabels(labels)
	if err != nil {
		return err
	}

	orderBy, err := parseOrderBy(sortBy, sortOrder)
	if err != nil {
		return err
	}

	spec := &stateless.QuerySpec{
		Pagination: &v1alphaquery.PaginationSpec{
			Offset:   offset,
			Limit:    limit,
			OrderBy:  orderBy,
			MaxLimit: maxLimit,
		},
		Labels:    pelotonLabels,
		Keywords:  parseKeyWords(keywords),
		JobStates: parseJobStates(states),
		Owner:     owner,
		Name:      name,
	}

	if len(respoolPath) > 0 {
		spec.Respool = &v1alpharespool.ResourcePoolPath{
			Value: respoolPath,
		}
	}

	if days > 0 {
		now := time.Now().UTC()
		max, err := ptypes.TimestampProto(now)
		if err != nil {
			return err
		}
		min, err := ptypes.TimestampProto(now.AddDate(0, 0, -int(days)))
		if err != nil {
			return err
		}
		spec.CreationTimeRange = &v1alphapeloton.TimeRange{Min: min, Max: max}
	}

	resp, err := c.statelessClient.QueryJobs(c.ctx, &statelesssvc.QueryJobsRequest{
		Spec: spec,
	})
	if err != nil {
		return err
	}
	printStatelessQueryResponse(resp)

	tabWriter.Flush()
	return nil
}

// StatelessReplaceJobAction updates job by replace its config
func (c *Client) StatelessReplaceJobAction(
	jobID string,
	spec string,
	batchSize uint32,
	respoolPath string,
	entityVersion string,
	override bool,
	maxInstanceRetries uint32,
	maxTolerableInstanceFailures uint32,
	rollbackOnFailure bool,
	startPaused bool,
	opaqueData string,
	inPlace bool,
	startPods bool,
) error {
	var jobSpec stateless.JobSpec

	// read the job configuration
	buffer, err := ioutil.ReadFile(spec)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %v", spec, err)
	}
	if err := yaml.Unmarshal(buffer, &jobSpec); err != nil {
		return fmt.Errorf("unable to parse file %s: %v", spec, err)
	}

	// fetch the resource pool id
	respoolID, err := c.LookupResourcePoolID(respoolPath)
	if err != nil {
		return err
	}
	if respoolID == nil {
		return fmt.Errorf("unable to find resource pool ID for "+
			":%s", respoolPath)
	}

	// Get the entity version if not provided as input.
	// The assumption is that the customer has ensured that
	// there is no other ongoing write API request going on.
	// In case the entityversion changes between Get and Replace,
	// then let the Replace request fail.
	if len(entityVersion) == 0 {
		getRequest := statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{
				Value: jobID,
			},
			SummaryOnly: true,
		}

		getResponse, err := c.statelessClient.GetJob(
			c.ctx,
			&getRequest)
		if err != nil {
			return err
		}

		entityVersion = getResponse.GetSummary().GetStatus().GetVersion().GetValue()
	}

	// set the resource pool id
	jobSpec.RespoolId = &v1alphapeloton.ResourcePoolID{Value: respoolID.GetValue()}

	var opaque *v1alphapeloton.OpaqueData
	if len(opaqueData) > 0 {
		opaque = &v1alphapeloton.OpaqueData{Data: opaqueData}
	}

	req := &statelesssvc.ReplaceJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: jobID},
		Version: &v1alphapeloton.EntityVersion{Value: entityVersion},
		Spec:    &jobSpec,
		UpdateSpec: &stateless.UpdateSpec{
			BatchSize:                    batchSize,
			RollbackOnFailure:            rollbackOnFailure,
			MaxInstanceRetries:           maxInstanceRetries,
			MaxTolerableInstanceFailures: maxTolerableInstanceFailures,
			StartPaused:                  startPaused,
			InPlace:                      inPlace,
			StartPods:                    startPods,
		},
		OpaqueData: opaque,
	}

	resp, err := c.statelessClient.ReplaceJob(c.ctx, req)
	if err != nil {
		return err
	}

	fmt.Printf("New EntityVersion: %s\n", resp.GetVersion().GetValue())

	return nil
}

// StatelessRollbackJobAction roll backs a job to an older entity version by
// getting the old configuration, and triggering a replace action
// with the old configiuration.
func (c *Client) StatelessRollbackJobAction(
	jobID string,
	batchSize uint32,
	entityVersion string,
	maxInstanceRetries uint32,
	maxTolerableInstanceFailures uint32,
	startPaused bool,
	opaqueData string,
	inPlace bool,
	startPods bool,
) error {
	// First fetch the previous job configuration.
	getConfigRequest := statelesssvc.GetJobRequest{
		JobId: &v1alphapeloton.JobID{
			Value: jobID,
		},
		Version: &v1alphapeloton.EntityVersion{
			Value: entityVersion,
		},
	}
	getConfigResponse, err := c.statelessClient.GetJob(
		c.ctx,
		&getConfigRequest,
	)
	if err != nil {
		return err
	}

	jobSpec := getConfigResponse.GetJobInfo().GetSpec()

	// Get the latest entity version.
	getRequest := statelesssvc.GetJobRequest{
		JobId: &v1alphapeloton.JobID{
			Value: jobID,
		},
		SummaryOnly: true,
	}

	getResponse, err := c.statelessClient.GetJob(
		c.ctx,
		&getRequest,
	)
	if err != nil {
		return err
	}

	version := getResponse.GetSummary().GetStatus().GetVersion().GetValue()

	jobSpec.Revision = nil

	var opaque *v1alphapeloton.OpaqueData
	if len(opaqueData) > 0 {
		opaque = &v1alphapeloton.OpaqueData{Data: opaqueData}
	}

	req := &statelesssvc.ReplaceJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: jobID},
		Version: &v1alphapeloton.EntityVersion{Value: version},
		Spec:    jobSpec,
		UpdateSpec: &stateless.UpdateSpec{
			BatchSize:                    batchSize,
			RollbackOnFailure:            false,
			MaxInstanceRetries:           maxInstanceRetries,
			MaxTolerableInstanceFailures: maxTolerableInstanceFailures,
			StartPaused:                  startPaused,
			InPlace:                      inPlace,
			StartPods:                    startPods,
		},
		OpaqueData: opaque,
	}

	resp, err := c.statelessClient.ReplaceJob(c.ctx, req)
	if err != nil {
		return err
	}

	fmt.Printf("New EntityVersion: %s\n", resp.GetVersion().GetValue())

	return nil
}

// StatelessListJobsAction prints summary of all jobs using the ListJobs API
func (c *Client) StatelessListJobsAction() error {
	defer tabWriter.Flush()
	stream, err := c.statelessClient.ListJobs(
		c.ctx,
		&statelesssvc.ListJobsRequest{},
	)
	if err != nil {
		return err
	}

	fmt.Fprint(tabWriter, statelessJobSummaryFormatHeader)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		printListJobsResponse(resp)
	}
}

// StatelessReplaceJobDiffAction returns the set of instances which will be
// added, removed, updated and remain unchanged for a new
// job specification for a given job
func (c *Client) StatelessReplaceJobDiffAction(
	jobID string,
	spec string,
	entityVersion string,
	respoolPath string,
) error {
	var jobSpec stateless.JobSpec

	// read the job configuration
	buffer, err := ioutil.ReadFile(spec)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %v", spec, err)
	}
	if err := yaml.Unmarshal(buffer, &jobSpec); err != nil {
		return fmt.Errorf("unable to parse file %s: %v", spec, err)
	}

	// fetch the resource pool id
	respoolID, err := c.LookupResourcePoolID(respoolPath)
	if err != nil {
		return err
	}
	if respoolID == nil {
		return fmt.Errorf("unable to find resource pool ID for "+
			":%s", respoolPath)
	}

	// set the resource pool id
	jobSpec.RespoolId = &v1alphapeloton.ResourcePoolID{Value: respoolID.GetValue()}

	req := &statelesssvc.GetReplaceJobDiffRequest{
		JobId:   &v1alphapeloton.JobID{Value: jobID},
		Spec:    &jobSpec,
		Version: &v1alphapeloton.EntityVersion{Value: entityVersion},
	}

	resp, err := c.statelessClient.GetReplaceJobDiff(c.ctx, req)
	if err != nil {
		return err
	}

	printResponseJSON(resp)
	return nil
}

// StatelessCreateAction is the action for creating a stateless job
func (c *Client) StatelessCreateAction(
	jobID string,
	respoolPath string,
	batchSize uint32,
	cfg string,
	secretPath string,
	secret []byte,
	opaque string,
	startPaused bool,
	maxInstanceRetries uint32,
	maxTolerableInstanceFailures uint32,
) error {
	respoolID, err := c.LookupResourcePoolID(respoolPath)
	if err != nil {
		return err
	}
	if respoolID == nil {
		return fmt.Errorf("unable to find resource pool ID for "+
			":%s", respoolPath)
	}

	var jobSpec stateless.JobSpec
	buffer, err := ioutil.ReadFile(cfg)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %v", cfg, err)
	}
	if err := yaml.Unmarshal(buffer, &jobSpec); err != nil {
		return fmt.Errorf("unable to parse file %s: %v", cfg, err)
	}

	jobSpec.RespoolId = &v1alphapeloton.ResourcePoolID{Value: respoolID.GetValue()}

	var opaqueData *v1alphapeloton.OpaqueData
	if len(opaque) != 0 {
		opaqueData = &v1alphapeloton.OpaqueData{
			Data: opaque,
		}
	}

	var request = &statelesssvc.CreateJobRequest{
		JobId: &v1alphapeloton.JobID{
			Value: jobID,
		},
		Spec:       &jobSpec,
		OpaqueData: opaqueData,
		CreateSpec: &stateless.CreateSpec{
			BatchSize:                    batchSize,
			MaxInstanceRetries:           maxInstanceRetries,
			MaxTolerableInstanceFailures: maxTolerableInstanceFailures,
			StartPaused:                  startPaused,
		},
	}

	// handle secrets
	if secretPath != "" && len(secret) > 0 {
		request.Secrets = []*v1alphapeloton.Secret{
			jobmgrtask.CreateV1AlphaSecretProto("", secretPath, secret),
		}
	}

	response, err := c.statelessClient.CreateJob(c.ctx, request)

	printStatelessJobCreateResponse(request, response, err, c.Debug)

	return err
}

// StatelessGetAction is the action for getting status
// and spec (or only summary) of a stateless job
func (c *Client) StatelessGetAction(
	jobID string,
	version string,
	summaryOnly bool,
) error {
	request := statelesssvc.GetJobRequest{
		JobId: &v1alphapeloton.JobID{
			Value: jobID,
		},
		SummaryOnly: summaryOnly,
	}

	if version != "" {
		request.Version = &v1alphapeloton.EntityVersion{
			Value: version,
		}
	}

	resp, err := c.statelessClient.GetJob(
		c.ctx,
		&request)
	if err != nil {
		return err
	}

	out, err := marshallResponse(defaultResponseFormat, resp)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", string(out))

	return nil
}

// StatelessQueryPodsAction is the action for querying pods of a job
func (c *Client) StatelessQueryPodsAction(
	jobID string,
	states string,
	names string,
	hosts string,
	limit uint32,
	offset uint32,
	sortBy string,
	sortOrder string,
) error {
	var podStates []v1alphapod.PodState
	var podNames []*v1alphapeloton.PodName
	var podHosts []string
	for _, k := range strings.Split(states, labelSeparator) {
		if k != "" {
			p, ok := v1alphapod.PodState_value[k]
			if !ok {
				return yarpcerrors.InvalidArgumentErrorf("invalid pod state %s", k)
			}
			podStates = append(podStates, v1alphapod.PodState(p))
		}
	}

	for _, host := range strings.Split(hosts, labelSeparator) {
		if host != "" {
			podHosts = append(podHosts, host)
		}
	}

	for _, name := range strings.Split(names, labelSeparator) {
		if name != "" {
			podNames = append(podNames, &v1alphapeloton.PodName{Value: name})
		}
	}

	order := v1alphaquery.OrderBy_ORDER_BY_DESC
	if sortOrder == "ASC" {
		order = v1alphaquery.OrderBy_ORDER_BY_ASC
	} else if sortOrder != "DESC" {
		return yarpcerrors.InvalidArgumentErrorf("Invalid sort order " + sortOrder)
	}

	var sort []*v1alphaquery.OrderBy
	for _, s := range strings.Split(sortBy, labelSeparator) {
		if s != "" {
			propertyPath := &v1alphaquery.PropertyPath{
				Value: s,
			}
			sort = append(sort, &v1alphaquery.OrderBy{
				Order:    order,
				Property: propertyPath,
			})
		}
	}

	var request = &statelesssvc.QueryPodsRequest{
		JobId: &v1alphapeloton.JobID{
			Value: jobID,
		},
		Spec: &v1alphapod.QuerySpec{
			PodStates: podStates,
			Names:     podNames,
			Hosts:     podHosts,
			Pagination: &v1alphaquery.PaginationSpec{
				Limit:   limit,
				Offset:  offset,
				OrderBy: sort,
			},
		},
		Pagination: &v1alphaquery.PaginationSpec{
			Limit:   limit,
			Offset:  offset,
			OrderBy: sort,
		},
	}

	response, err := c.statelessClient.QueryPods(c.ctx, request)

	printQueryPodsResponse(response, err, c.Debug)

	return err
}

// StatelessListPodsAction is the action to list pods in a job
func (c *Client) StatelessListPodsAction(
	jobID string,
	instanceRange *task.InstanceRange,
) error {
	defer tabWriter.Flush()
	idInstanceRange := &v1alphapod.InstanceIDRange{
		From: instanceRange.GetFrom(),
		To:   instanceRange.GetTo(),
	}

	stream, err := c.statelessClient.ListPods(
		c.ctx,
		&statelesssvc.ListPodsRequest{
			JobId: &v1alphapeloton.JobID{Value: jobID},
			Range: idInstanceRange,
		},
	)

	if err != nil {
		return err
	}

	fmt.Fprint(tabWriter, queryPodsFormatHeader)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		printStatelessListPodsResponse(resp)
	}
}

func printStatelessListPodsResponse(resp *statelesssvc.ListPodsResponse) {
	for _, pod := range resp.GetPods() {
		printPod(pod.GetStatus(), pod.GetPodName())
	}
}

// StatelessStartJobAction is the action for starting a stateless job
func (c *Client) StatelessStartJobAction(
	jobID string,
	entityVersion string,
) error {
	request := statelesssvc.StartJobRequest{
		JobId: &v1alphapeloton.JobID{
			Value: jobID,
		},
		Version: &v1alphapeloton.EntityVersion{
			Value: entityVersion,
		},
	}

	resp, err := c.statelessClient.StartJob(
		c.ctx,
		&request)

	if err != nil {
		return err
	}

	fmt.Printf("Job started. New EntityVersion: %s\n", resp.GetVersion().GetValue())

	return nil
}

// StatelessDeleteAction is the action for deleting a stateless job
func (c *Client) StatelessDeleteAction(
	jobID string,
	version string,
	forceDelete bool,
) error {
	_, err := c.statelessClient.DeleteJob(
		c.ctx,
		&statelesssvc.DeleteJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: jobID},
			Version: &v1alphapeloton.EntityVersion{Value: version},
			Force:   forceDelete,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Job deleted\n")
	return nil
}

func printStatelessQueryResponse(resp *statelesssvc.QueryJobsResponse) {
	results := resp.GetRecords()
	if len(results) == 0 {
		fmt.Fprintf(tabWriter, "No results found\n")
		return
	}

	fmt.Fprint(tabWriter, statelessJobSummaryFormatHeader)
	for _, r := range results {
		printStatelessQueryResult(r)
	}
}

func printStatelessQueryResult(j *stateless.JobSummary) {
	creationTime, err := time.Parse(time.RFC3339Nano, j.GetStatus().GetCreationTime())
	creationTimeStr := ""
	if err == nil {
		creationTimeStr = creationTime.Format(time.RFC3339)
	}

	fmt.Fprintf(
		tabWriter,
		statelessSummaryFormatBody,
		j.GetJobId().GetValue(),
		j.GetName(),
		j.GetOwningTeam(),
		j.GetStatus().GetState().String(),
		creationTimeStr,
		j.GetInstanceCount(),
		j.GetStatus().GetPodStats()["POD_STATE_RUNNING"],
		j.GetStatus().GetPodStats()["POD_STATE_SUCCEEDED"],
		j.GetStatus().GetPodStats()["POD_STATE_FAILED"],
		j.GetStatus().GetPodStats()["POD_STATE_KILLED"],
		j.GetStatus().GetWorkflowStatus().GetState().String(),
		j.GetStatus().GetWorkflowStatus().GetNumInstancesCompleted(),
		j.GetStatus().GetWorkflowStatus().GetNumInstancesFailed(),
		len(j.GetStatus().GetWorkflowStatus().GetInstancesCurrent()),
	)
}

func parseLabels(labels string) ([]*v1alphapeloton.Label, error) {
	var pelotonLabels []*v1alphapeloton.Label
	if len(labels) == 0 {
		return nil, nil
	}

	for _, l := range strings.Split(labels, labelSeparator) {
		labelVals := strings.Split(l, keyValSeparator)
		if len(labelVals) != 2 {
			return nil, errors.New("Invalid label" + l)
		}
		pelotonLabels = append(pelotonLabels, &v1alphapeloton.Label{
			Key:   labelVals[0],
			Value: labelVals[1],
		})
	}
	return pelotonLabels, nil
}

func parseKeyWords(keywords string) []string {
	if len(keywords) == 0 {
		return nil
	}

	var pelotonKeyWords []string
	for _, k := range strings.Split(keywords, labelSeparator) {
		if k != "" {
			pelotonKeyWords = append(pelotonKeyWords, k)
		}
	}
	return pelotonKeyWords
}

func parseJobStates(states string) []stateless.JobState {
	if len(states) == 0 {
		return nil
	}

	var pelotonStates []stateless.JobState
	for _, k := range strings.Split(states, labelSeparator) {
		if k != "" {
			pelotonStates = append(pelotonStates, stateless.JobState(stateless.JobState_value[k]))
		}
	}
	return pelotonStates
}

func parseOrderBy(sortBy string, sortOrder string) ([]*v1alphaquery.OrderBy, error) {
	if len(sortBy) == 0 || len(sortOrder) == 0 {
		return nil, nil
	}

	order := v1alphaquery.OrderBy_ORDER_BY_DESC
	if sortOrder == "ASC" {
		order = v1alphaquery.OrderBy_ORDER_BY_ASC
	} else if sortOrder != "DESC" {
		return nil, errors.New("Invalid sort order " + sortOrder)
	}
	var sort []*v1alphaquery.OrderBy
	for _, s := range strings.Split(sortBy, labelSeparator) {
		if s != "" {

			propertyPath := &v1alphaquery.PropertyPath{
				Value: s,
			}
			sort = append(sort, &v1alphaquery.OrderBy{
				Order:    order,
				Property: propertyPath,
			})
		}
	}
	return sort, nil
}

func printStatelessJobCreateResponse(
	req *statelesssvc.CreateJobRequest,
	resp *statelesssvc.CreateJobResponse,
	err error,
	jsonFormat bool,
) {
	defer tabWriter.Flush()
	if jsonFormat {
		printResponseJSON(resp)
	} else {
		if err != nil {
			if yarpcerrors.IsAlreadyExists(err) {
				fmt.Fprintf(tabWriter, "Job %s already exists: %s\n",
					req.GetJobId(), err.Error())
			} else if yarpcerrors.IsInvalidArgument(err) {
				fmt.Fprintf(tabWriter, "Invalid job spec: %s\n",
					err.Error())
			}
		} else if resp.GetJobId() != nil {
			fmt.Fprintf(
				tabWriter,
				"Job %s created. Entity Version: %s\n",
				resp.GetJobId().GetValue(),
				resp.GetVersion().GetValue(),
			)
		} else {
			fmt.Fprint(tabWriter, "Missing job ID in job create response\n")
		}
	}
}

func printListJobsResponse(resp *statelesssvc.ListJobsResponse) {
	jobs := resp.GetJobs()
	for _, r := range jobs {
		printStatelessQueryResult(r)
	}
}

// StatelessRestartJobAction restarts a job
func (c *Client) StatelessRestartJobAction(
	jobID string,
	batchSize uint32,
	entityVersion string,
	instanceRanges []*task.InstanceRange,
	opaqueData string,
	inPlace bool,
) error {
	var opaque *v1alphapeloton.OpaqueData
	if len(opaqueData) > 0 {
		opaque = &v1alphapeloton.OpaqueData{Data: opaqueData}
	}

	var idInstanceRanges []*v1alphapod.InstanceIDRange
	for _, instanceRange := range instanceRanges {
		idInstanceRanges = append(idInstanceRanges, &v1alphapod.InstanceIDRange{
			From: instanceRange.GetFrom(),
			To:   instanceRange.GetTo(),
		})
	}

	req := &statelesssvc.RestartJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: jobID},
		Version: &v1alphapeloton.EntityVersion{Value: entityVersion},
		RestartSpec: &stateless.RestartSpec{
			BatchSize: batchSize,
			Ranges:    idInstanceRanges,
			InPlace:   inPlace,
		},
		OpaqueData: opaque,
	}

	resp, err := c.statelessClient.RestartJob(c.ctx, req)
	if err != nil {
		return err
	}

	fmt.Printf("Job restarted. New EntityVersion: %s\n", resp.GetVersion().GetValue())
	return nil
}

// StatelessListUpdatesAction lists updates of a job
func (c *Client) StatelessListUpdatesAction(
	jobID string,
	updatesLimit uint32,
) error {
	resp, err := c.statelessClient.ListJobWorkflows(
		c.ctx,
		&statelesssvc.ListJobWorkflowsRequest{
			JobId:        &v1alphapeloton.JobID{Value: jobID},
			UpdatesLimit: updatesLimit,
		},
	)

	if err != nil {
		return err
	}

	return printListUpdatesResponse(resp, c.Debug)
}

func printListUpdatesResponse(
	resp *statelesssvc.ListJobWorkflowsResponse,
	debug bool) error {
	updates := resp.GetWorkflowInfos()

	if debug {
		printResponseJSON(resp)
		return nil
	}

	if len(updates) == 0 {
		fmt.Println("No update for job")
	} else {
		for i, u := range updates {
			fmt.Printf("Index %d:\n", i)
			if err := printUpdateInfo(u); err != nil {
				return err
			}
			fmt.Printf("\n\n\n")
		}
	}

	return nil
}

func printUpdateInfo(workflowInfo *stateless.WorkflowInfo) error {
	out, err := marshallResponse(defaultResponseFormat, workflowInfo)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", string(out))

	if len(workflowInfo.GetEvents()) != 0 {
		fmt.Fprint(tabWriter, statelessUpdateEventsFormatHeader)
		for _, event := range workflowInfo.GetEvents() {
			fmt.Fprintf(
				tabWriter,
				statelessUpdateEventsFormatBody,
				event.GetType().String(),
				event.GetTimestamp(),
				event.GetState().String(),
			)
		}
		tabWriter.Flush()
	}
	return nil
}

// StatelessWorkflowEventsAction gets most recent active or
// completed workflow events for a job
func (c *Client) StatelessWorkflowEventsAction(
	jobID string,
	instanceID uint32,
) error {
	resp, err := c.statelessClient.GetWorkflowEvents(
		c.ctx,
		&statelesssvc.GetWorkflowEventsRequest{
			JobId:      &v1alphapeloton.JobID{Value: jobID},
			InstanceId: instanceID,
		})

	if err != nil {
		return err
	}

	printWorkflowEventsV1AlphaResponse(resp)
	return nil
}

func printWorkflowEventsV1AlphaResponse(r *statelesssvc.GetWorkflowEventsResponse) {
	defer tabWriter.Flush()

	fmt.Fprint(tabWriter, workflowEventsV1AlphaFormatHeader)
	for _, event := range r.GetEvents() {
		fmt.Fprintf(
			tabWriter,
			workflowEventsV1AlphaFormatBody,
			event.GetState(),
			event.GetType(),
			event.GetTimestamp(),
		)
	}
}

func printQueryPodsResponse(r *statelesssvc.QueryPodsResponse, err error, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(r)
		return
	}

	if yarpcerrors.IsNotFound(err) {
		fmt.Fprintf(tabWriter, "Job was not found\n")
		return
	}

	if len(r.GetPods()) == 0 {
		fmt.Fprint(tabWriter, "No pods found\n")
		return
	}

	fmt.Fprint(tabWriter, queryPodsFormatHeader)
	for _, t := range r.GetPods() {
		printPod(t.GetStatus(), t.GetSpec().GetPodName())
	}
}

func printPod(status *v1alphapod.PodStatus, podName *v1alphapeloton.PodName) {
	for _, container := range status.GetContainersStatus() {
		// Calculate the start time and run time of the container
		startTimeStr := ""
		durationStr := ""
		startTime, err := time.Parse(time.RFC3339Nano, container.GetStartTime())
		if err == nil {
			startTimeStr = startTime.Format(time.RFC3339)
			completionTime, err := time.Parse(time.RFC3339Nano, container.GetCompletionTime())
			var duration time.Duration
			if err == nil {
				duration = completionTime.Sub(startTime)
			} else {
				duration = time.Now().Sub(startTime)
			}
			durationStr = fmt.Sprintf(
				"%02d:%02d:%02d",
				uint(duration.Hours()),
				uint(duration.Minutes())%60,
				uint(duration.Seconds())%60,
			)
		}
		termStatusStr := ""
		termStatus := container.GetTerminationStatus()
		if termStatus != nil {
			termStatusStr = termStatus.GetReason().String()
			termStatusStr = strings.TrimPrefix(termStatusStr, "TERMINATION_STATUS_REASON_")
		}
		// Print the container record
		fmt.Fprintf(
			tabWriter,
			queryPodsFormatBody,
			status.GetPodId().GetValue(),
			podName.GetValue(),
			status.GetState().String(),
			container.GetName(),
			container.GetState().String(),
			container.GetHealthy().GetState().String(),
			startTimeStr,
			durationStr,
			status.GetHost(),
			container.GetMessage(),
			container.GetReason(),
			termStatusStr,
		)
	}
}
