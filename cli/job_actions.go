package cli

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/util"

	"github.com/golang/protobuf/ptypes"
	"gopkg.in/cheggaaa/pb.v1"
	"gopkg.in/yaml.v2"
)

const (
	labelSeparator  = ","
	keyValSeparator = "="

	defaultResponseFormat = "yaml"
	jsonResponseFormat    = "json"

	jobStopProgressTimeout = 10 * time.Minute
	jobStopProgressRefresh = 5 * time.Second

	jobSummaryFormatHeader = "ID\tName\tOwner\tState\tCreation Time\tCompletion Time\tTotal\t" +
		"Running\tSucceeded\tFailed\tKilled\t\n"
	jobSummaryFormatBody = "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t\n"
)

// JobCreateAction is the action for creating a job
func (c *Client) JobCreateAction(
	jobID, respoolPath, cfg, secretPath string, secret []byte,
) error {
	respoolID, err := c.LookupResourcePoolID(respoolPath)
	if err != nil {
		return err
	}
	if respoolID == nil {
		return fmt.Errorf("unable to find resource pool ID for "+
			":%s", respoolPath)
	}

	var jobConfig job.JobConfig
	buffer, err := ioutil.ReadFile(cfg)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %v", cfg, err)
	}
	if err := yaml.Unmarshal(buffer, &jobConfig); err != nil {
		return fmt.Errorf("unable to parse file %s: %v", cfg, err)
	}

	// TODO remove this once respool is moved out of jobconfig
	// set the resource pool ID
	jobConfig.RespoolID = respoolID

	var request = &job.CreateRequest{
		Id: &peloton.JobID{
			Value: jobID,
		},
		Config: &jobConfig,
	}
	// handle secrets
	if secretPath != "" && len(secret) > 0 {
		request.Secrets = []*peloton.Secret{
			jobmgrtask.CreateSecretProto("", secretPath, secret)}
	}

	response, err := c.jobClient.Create(c.ctx, request)
	if err != nil {
		return err
	}
	printJobCreateResponse(response, c.Debug)
	return nil
}

// JobDeleteAction is the action for deleting a job
func (c *Client) JobDeleteAction(jobID string) error {
	var request = &job.DeleteRequest{
		Id: &peloton.JobID{
			Value: jobID,
		},
	}
	response, err := c.jobClient.Delete(c.ctx, request)
	if err != nil {
		return err
	}
	printResponseJSON(response)
	return nil
}

// JobGetAction is the action for getting a job
func (c *Client) JobGetAction(jobID string) error {
	response, err := c.jobGet(jobID)
	if err != nil {
		return err
	}
	printJobGetResponse(response, c.Debug)
	return nil
}

func (c *Client) jobGet(jobID string) (*job.GetResponse, error) {
	var request = &job.GetRequest{
		Id: &peloton.JobID{
			Value: jobID,
		},
	}
	return c.jobClient.Get(c.ctx, request)
}

// JobGetCacheAction is the action for getting a job cache
func (c *Client) JobGetCacheAction(jobID string) error {
	r, err := c.jobClient.GetCache(c.ctx, &job.GetCacheRequest{
		Id: &peloton.JobID{Value: jobID},
	})
	if err != nil {
		return err
	}

	printResponseJSON(r)
	tabWriter.Flush()
	return nil
}

// JobRefreshAction calls the refresh API for a job
func (c *Client) JobRefreshAction(jobID string) error {
	var request = &job.RefreshRequest{
		Id: &peloton.JobID{
			Value: jobID,
		},
	}
	_, err := c.jobClient.Refresh(c.ctx, request)
	return err
}

// JobStatusAction is the action for getting status of a job
func (c *Client) JobStatusAction(jobID string) error {
	var request = &job.GetRequest{
		Id: &peloton.JobID{
			Value: jobID,
		},
	}
	response, err := c.jobClient.Get(c.ctx, request)
	if err != nil {
		return err
	}
	printJobStatusResponse(response, c.Debug)
	return nil
}

// JobQueryAction is the action for getting job ids by labels and respool path
func (c *Client) JobQueryAction(
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
	var apiLabels []*peloton.Label
	if len(labels) > 0 {
		labelPairs := strings.Split(labels, labelSeparator)
		for _, l := range labelPairs {
			labelVals := strings.Split(l, keyValSeparator)
			if len(labelVals) != 2 {
				fmt.Printf("Invalid label %v", l)
				return errors.New("Invalid label" + l)
			}
			apiLabels = append(apiLabels, &peloton.Label{
				Key:   labelVals[0],
				Value: labelVals[1],
			})
		}
	}
	var respoolID *peloton.ResourcePoolID
	var err error
	if len(respoolPath) > 0 {
		respoolID, err = c.LookupResourcePoolID(respoolPath)
		if err != nil {
			return err
		}
	}
	var apiKeywords []string
	for _, k := range strings.Split(keywords, labelSeparator) {
		if k != "" {
			apiKeywords = append(apiKeywords, k)
		}
	}

	var apiStates []job.JobState
	for _, k := range strings.Split(states, labelSeparator) {
		if k != "" {
			apiStates = append(apiStates, job.JobState(job.JobState_value[k]))
		}
	}

	order := query.OrderBy_DESC
	if sortOrder == "ASC" {
		order = query.OrderBy_ASC
	} else if sortOrder != "DESC" {
		return errors.New("Invalid sort order " + sortOrder)
	}
	var sort []*query.OrderBy
	for _, s := range strings.Split(sortBy, labelSeparator) {
		if s != "" {

			propertyPath := &query.PropertyPath{
				Value: s,
			}
			sort = append(sort, &query.OrderBy{
				Order:    order,
				Property: propertyPath,
			})
		}
	}

	spec := &job.QuerySpec{
		Labels:    apiLabels,
		Keywords:  apiKeywords,
		JobStates: apiStates,
		Owner:     owner,
		Name:      name,
		Pagination: &query.PaginationSpec{
			Limit:    limit,
			Offset:   offset,
			OrderBy:  sort,
			MaxLimit: maxLimit,
		},
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
		spec.CreationTimeRange = &peloton.TimeRange{Min: min, Max: max}
	}

	var request = &job.QueryRequest{
		RespoolID:   respoolID,
		Spec:        spec,
		SummaryOnly: true,
	}
	response, err := c.jobClient.Query(c.ctx, request)
	if err != nil {
		return err
	}
	printJobQueryResponse(response, c.Debug)
	return nil
}

// JobUpdateAction is the action of updating a job
func (c *Client) JobUpdateAction(
	jobID, cfg, secretPath string, secret []byte) error {
	var jobConfig job.JobConfig
	buffer, err := ioutil.ReadFile(cfg)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %v", cfg, err)
	}
	if err := yaml.Unmarshal(buffer, &jobConfig); err != nil {
		return fmt.Errorf("unable to parse file %s: %v", cfg, err)
	}

	var request = &job.UpdateRequest{
		Id: &peloton.JobID{
			Value: jobID,
		},
		Config: &jobConfig,
	}
	// handle secrets
	if secretPath != "" && len(secret) > 0 {
		request.Secrets = []*peloton.Secret{
			jobmgrtask.CreateSecretProto("", secretPath, secret)}
	}
	response, err := c.jobClient.Update(c.ctx, request)
	if err != nil {
		return err
	}

	printJobUpdateResponse(response, c.Debug)
	return nil
}

// JobStopAction is the action of stopping a job
func (c *Client) JobStopAction(jobID string, showProgress bool) error {
	// check the job status
	jobGetResponse, err := c.jobGet(jobID)
	if err != nil {
		return err
	}

	if util.IsPelotonJobStateTerminal(
		jobGetResponse.GetJobInfo().GetRuntime().GetState()) {
		fmt.Fprintf(
			tabWriter,
			"Job is in terminal state: %s\n", jobGetResponse.GetJobInfo().
				GetRuntime().GetState().String(),
		)
		tabWriter.Flush()
		return nil
	}

	id := &peloton.JobID{
		Value: jobID,
	}

	// job is not in terminal state, so lets stop it
	var request = &task.StopRequest{
		JobId:  id,
		Ranges: nil,
	}
	response, err := c.taskClient.Stop(c.ctx, request)
	if err != nil {
		return err
	}

	if showProgress {
		return c.pollStatusWithTimeout(id)
	}

	printTaskStopResponse(response, c.Debug)
	// Retry one more time in case failedInstanceList is non zero
	if len(response.GetInvalidInstanceIds()) > 0 {
		fmt.Fprint(
			tabWriter,
			"Retrying failed tasks",
		)
		response, err = c.taskClient.Stop(c.ctx, request)
		if err != nil {
			return err
		}
		printTaskStopResponse(response, c.Debug)
	}
	return nil
}

func (c *Client) pollStatusWithTimeout(id *peloton.JobID) error {
	// get the status
	total, terminated, err := c.tasksTerminated(id)
	if err != nil {
		return err
	}

	if total == terminated {
		// done
		fmt.Printf("Job stopped, total_tasks:%d terminated_tasks:%d\n",
			total, terminated)
		return nil
	}

	// init the bar to the total tasks
	bar := pb.Simple.
		Start(int(total)).
		SetTotal(int64(total)).
		SetWidth(80).
		SetRefreshRate(time.Second).Set("prefix", "terminated/total: ")
	defer bar.Finish()

	// Keep trying until we're timed out or got a result or got an error
	timeout := time.After(jobStopProgressTimeout)
	refresh := time.Tick(jobStopProgressRefresh)
	for {
		select {
		case <-timeout:
			fmt.Fprint(os.Stderr, "Timed out waiting for job to stop")
			return nil
		case <-refresh:
			total, terminated, err := c.tasksTerminated(id)
			if err != nil {
				return err
			}

			bar.SetCurrent(int64(terminated))
			if total == terminated {
				// done
				return nil
			}
		}
	}
}

// returns the total number of tasks, terminated tasks and error if any
func (c *Client) tasksTerminated(id *peloton.JobID) (uint32, uint32, error) {
	var request = &job.GetRequest{
		Id: id,
	}
	ctx, cf := context.WithTimeout(context.Background(), 5*time.Second)
	defer cf()

	response, err := c.jobClient.Get(ctx, request)
	if err != nil {
		return 0, 0, err
	}

	total := response.GetJobInfo().GetConfig().GetInstanceCount()
	// check job state
	state := response.GetJobInfo().GetRuntime().GetState()
	if util.IsPelotonJobStateTerminal(state) {
		// the job is in terminal state
		return total, total, nil
	}

	tasksInStates := response.GetJobInfo().GetRuntime().GetTaskStats()
	// all terminal task states
	terminated := uint32(0)
	for state, count := range tasksInStates {
		if util.IsPelotonStateTerminal(task.TaskState(task.
			TaskState_value[state])) {
			terminated += count
		}
	}

	return total, terminated, nil
}

func printJobUpdateResponse(r *job.UpdateResponse, jsonFormat bool) {
	if jsonFormat {
		printResponseJSON(r)
	} else {
		if r.Error != nil {
			if r.Error.JobNotFound != nil {
				fmt.Fprintf(tabWriter, "Job %s not found: %s\n",
					r.Error.JobNotFound.Id.Value, r.Error.JobNotFound.Message)
			} else if r.Error.InvalidConfig != nil {
				fmt.Fprintf(tabWriter, "Invalid job config: %s\n",
					r.Error.InvalidConfig.Message)
			}
		} else if r.Id != nil {
			fmt.Fprintf(tabWriter, "Job %s updated\n", r.Id.Value)
			fmt.Fprint(tabWriter, "Message:", r.Message)
		}
	}
}

func printJobCreateResponse(r *job.CreateResponse, jsonFormat bool) {
	if jsonFormat {
		printResponseJSON(r)
	} else {
		if r.Error != nil {
			if r.Error.AlreadyExists != nil {
				fmt.Fprintf(tabWriter, "Job %s already exists: %s\n",
					r.Error.AlreadyExists.Id.Value, r.Error.AlreadyExists.Message)
			} else if r.Error.InvalidConfig != nil {
				fmt.Fprintf(tabWriter, "Invalid job config: %s\n",
					r.Error.InvalidConfig.Message)
			} else if r.Error.InvalidJobId != nil {
				fmt.Fprintf(tabWriter, "Invalid job ID: %v, message: %v\n",
					r.Error.InvalidJobId.Id.Value,
					r.Error.InvalidJobId.Message)
			}
		} else if r.JobId != nil {
			fmt.Fprintf(tabWriter, "Job %s created\n", r.JobId.Value)
		} else {
			fmt.Fprint(tabWriter, "Missing job ID in job create response\n")
		}
		tabWriter.Flush()
	}
}

func printJobGetResponse(r *job.GetResponse, jsonFormat bool) {
	if r.GetJobInfo() == nil {
		fmt.Fprint(tabWriter, "Unable to get job \n")
	} else {
		format := defaultResponseFormat
		if jsonFormat {
			format = jsonResponseFormat
		}
		out, err := marshallResponse(format, r)
		if err != nil {
			fmt.Fprint(tabWriter, "Unable to marshall response \n")
		}

		fmt.Printf("%v\n", string(out))
	}
	tabWriter.Flush()
}

func printJobStatusResponse(r *job.GetResponse, jsonFormat bool) {
	if r.GetJobInfo() == nil || r.GetJobInfo().GetRuntime() == nil {
		fmt.Fprint(tabWriter, "Unable to get job status\n")
	} else {
		ri := r.GetJobInfo().GetRuntime()
		format := defaultResponseFormat
		if jsonFormat {
			format = jsonResponseFormat
		}
		out, err := marshallResponse(format, ri)

		if err != nil {
			fmt.Fprint(tabWriter, "Unable to marshall response\n")
			return
		}

		fmt.Printf("%v\n", string(out))
	}
	tabWriter.Flush()
}

func printJobQueryResult(j *job.JobSummary) {
	creationTime, err := time.Parse(time.RFC3339Nano, j.GetRuntime().GetCreationTime())
	creationTimeStr := ""
	if err == nil {
		creationTimeStr = creationTime.Format(time.RFC3339)
	}
	completionTime, err := time.Parse(time.RFC3339Nano, j.GetRuntime().GetCompletionTime())
	completionTimeStr := ""
	if err == nil {
		completionTimeStr = completionTime.Format(time.RFC3339)
	} else {
		// completionTime will be empty for active jobs
		completionTimeStr = "--"
	}

	fmt.Fprintf(
		tabWriter,
		jobSummaryFormatBody,
		j.GetId().GetValue(),
		j.GetName(),
		j.GetOwningTeam(),
		j.GetRuntime().GetState().String(),
		creationTimeStr,
		completionTimeStr,
		j.GetInstanceCount(),
		j.GetRuntime().GetTaskStats()["RUNNING"],
		j.GetRuntime().GetTaskStats()["SUCCEEDED"],
		j.GetRuntime().GetTaskStats()["FAILED"],
		j.GetRuntime().GetTaskStats()["KILLED"],
	)
}

func printJobQueryResponse(r *job.QueryResponse, jsonFormat bool) {
	if jsonFormat {
		printResponseJSON(r)
	} else {
		if r.GetError() != nil {
			fmt.Fprintf(tabWriter, "Error: %v\n", r.GetError().String())
		} else {
			results := r.GetResults()
			if len(results) != 0 {
				fmt.Fprint(tabWriter, jobSummaryFormatHeader)
				for _, k := range results {
					printJobQueryResult(k)
				}
			} else {
				fmt.Fprint(tabWriter, "No jobs found.\n", r.GetError().String())
			}
		}
		tabWriter.Flush()
	}
}
