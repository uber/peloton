package cli

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	pelotonv1alphaquery "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/query"
	pelotonv1alpharespool "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/respool"

	"github.com/golang/protobuf/ptypes"
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

	tabWriter.Flush()
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

	tabWriter.Flush()
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
	}

	resp, err := c.statelessClient.ReplaceJob(c.ctx, req)
	if err != nil {
		return err
	}

	fmt.Printf("New EntityVersion: %s\n", resp.GetVersion().GetValue())

	return nil
}
