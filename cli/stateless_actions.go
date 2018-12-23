package cli

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	pelotonv1alphapod "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	pelotonv1alphaquery "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/query"
	pelotonv1alpharespool "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/respool"

	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"

	"github.com/golang/protobuf/ptypes"
	"go.uber.org/yarpc/yarpcerrors"
	"gopkg.in/yaml.v2"
)

const (
	statlessJobSummaryFormatHeader = "ID\tName\tOwner\tState\tCreation Time\tTotal\t" +
		"Running\tSucceeded\tFailed\tKilled\t" +
		"Workflow Status\tCompleted\tFailed\tCurrent\n"
	statelessSummaryFormatBody = "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t\n"
)

// StatelessGetCacheAction get cache of stateless job
func (c *Client) StatelessGetCacheAction(jobID string) error {
	resp, err := c.statelessClient.GetJobCache(
		c.ctx,
		&statelesssvc.GetJobCacheRequest{
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
	resp, err := c.statelessClient.RefreshJob(
		c.ctx,
		&statelesssvc.RefreshJobRequest{
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
		Pagination: &pelotonv1alphaquery.PaginationSpec{
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
		spec.Respool = &pelotonv1alpharespool.ResourcePoolPath{
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
) error {
	// TODO: implement cli override check and get entity version
	// form job after stateless.Get is ready
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
	stream, err := c.statelessClient.ListJobs(
		c.ctx,
		&statelesssvc.ListJobsRequest{},
	)
	if err != nil {
		return err
	}

	fmt.Fprint(tabWriter, statlessJobSummaryFormatHeader)
	tabWriter.Flush()
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		printListJobsResponse(resp)
		tabWriter.Flush()
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
	cfg string,
	secretPath string,
	secret []byte,
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

	var request = &statelesssvc.CreateJobRequest{
		JobId: &v1alphapeloton.JobID{
			Value: jobID,
		},
		Spec: &jobSpec,
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

func printStatelessQueryResponse(resp *statelesssvc.QueryJobsResponse) {
	results := resp.GetRecords()
	if len(results) == 0 {
		fmt.Fprintf(tabWriter, "No results found\n")
		return
	}

	fmt.Fprint(tabWriter, statlessJobSummaryFormatHeader)
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
		j.GetStatus().GetPodStats()["RUNNING"],
		j.GetStatus().GetPodStats()["SUCCEEDED"],
		j.GetStatus().GetPodStats()["FAILED"],
		j.GetStatus().GetPodStats()["KILLED"],
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

func parseOrderBy(sortBy string, sortOrder string) ([]*pelotonv1alphaquery.OrderBy, error) {
	if len(sortBy) == 0 || len(sortOrder) == 0 {
		return nil, nil
	}

	order := pelotonv1alphaquery.OrderBy_ORDER_BY_DESC
	if sortOrder == "ASC" {
		order = pelotonv1alphaquery.OrderBy_ORDER_BY_ASC
	} else if sortOrder != "DESC" {
		return nil, errors.New("Invalid sort order " + sortOrder)
	}
	var sort []*pelotonv1alphaquery.OrderBy
	for _, s := range strings.Split(sortBy, labelSeparator) {
		if s != "" {

			propertyPath := &pelotonv1alphaquery.PropertyPath{
				Value: s,
			}
			sort = append(sort, &pelotonv1alphaquery.OrderBy{
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
) error {
	var opaque *v1alphapeloton.OpaqueData
	if len(opaqueData) > 0 {
		opaque = &v1alphapeloton.OpaqueData{Data: opaqueData}
	}

	var idInstanceRanges []*pelotonv1alphapod.InstanceIDRange
	for _, instanceRange := range instanceRanges {
		idInstanceRanges = append(idInstanceRanges, &pelotonv1alphapod.InstanceIDRange{
			From: instanceRange.GetFrom(),
			To:   instanceRange.GetTo(),
		})
	}

	req := &statelesssvc.RestartJobRequest{
		JobId:      &v1alphapeloton.JobID{Value: jobID},
		Version:    &v1alphapeloton.EntityVersion{Value: entityVersion},
		BatchSize:  batchSize,
		Ranges:     idInstanceRanges,
		OpaqueData: opaque,
	}

	resp, err := c.statelessClient.RestartJob(c.ctx, req)
	if err != nil {
		return err
	}

	fmt.Printf("Job restarted. New EntityVersion: %s\n", resp.GetVersion().GetValue())
	return nil
}
