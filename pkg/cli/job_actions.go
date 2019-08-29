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
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/query"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common/stringset"
	"github.com/uber/peloton/pkg/common/util"
	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"

	"github.com/golang/protobuf/ptypes"
	"go.uber.org/multierr"
	"go.uber.org/yarpc/yarpcerrors"
	pb "gopkg.in/cheggaaa/pb.v1"
	yaml "gopkg.in/yaml.v2"
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

	jobStopConfirmationMessage = "The above jobs will be stopped. " +
		"Are you sure you want to continue?"
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

// JobGetActiveJobsAction is the action for getting active jobs list
func (c *Client) JobGetActiveJobsAction() error {
	r, err := c.jobClient.GetActiveJobs(c.ctx, &job.GetActiveJobsRequest{})
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

// JobQueryAction is the action for getting job ids by labels,
// respool path, keywords, state(s), owner and jobname
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
	var err error
	if len(labels) > 0 {
		apiLabels, err = parsePelotonLabels(labels)
		if err != nil {
			return err
		}
	}

	var respoolID *peloton.ResourcePoolID
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

// JobStopAction is the action of stopping job(s) by jobID,
// owner and labels
func (c *Client) JobStopAction(
	jobID string,
	showProgress bool,
	owner string,
	labels string,
	isForceStop bool,
	jobStopLimit uint32,
	jobStopMaxLimit uint32,
) error {
	var (
		errs             error
		jobsToStop       []*peloton.JobID
		jobStopLabels    []*peloton.Label
		stopJobIDs       []string
		stopFailedJobIDs []string
	)

	jobStopLabelSet := stringset.New()
	if len(labels) > 0 {
		var err error
		jobStopLabels, err = parsePelotonLabels(labels)
		if err != nil {
			return err
		}
		for _, label := range jobStopLabels {
			jobStopLabelSet.Add(label.GetKey() + ":" + label.GetValue())
		}
	}

	if jobID == "" && owner == "" {
		fmt.Printf("Either jobID or owner needs to be provided.\n")
		tabWriter.Flush()
		return nil
	}

	if jobID != "" {
		// check the job status
		jobGetResponse, err := c.jobGet(jobID)
		if err != nil {
			return err
		}

		jobConfig := jobGetResponse.GetJobInfo().GetConfig()
		// If the job doesn't satisfy owner or label constraints, return
		if owner != "" && jobConfig.GetOwningTeam() != owner {
			fmt.Printf("No matching job found\n")
			tabWriter.Flush()
			return nil
		}
		if len(labels) > 0 {
			jobHasStopLabels := false
			for _, label := range jobConfig.Labels {
				if jobStopLabelSet.Contains(strings.TrimSpace(label.GetKey()) +
					":" + strings.TrimSpace(label.GetValue())) {
					jobHasStopLabels = true
					break
				}
			}
			if !jobHasStopLabels {
				fmt.Fprintf(
					tabWriter,
					"No matching job found\n",
				)
				tabWriter.Flush()
				return nil
			}
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

		jobsToStop = append(jobsToStop, &peloton.JobID{Value: jobID})
	} else {
		jobStates := []job.JobState{
			job.JobState_INITIALIZED,
			job.JobState_PENDING,
			job.JobState_RUNNING,
		}
		spec := &job.QuerySpec{
			JobStates: jobStates,
			Owner:     owner,
			Labels:    jobStopLabels,
			Pagination: &query.PaginationSpec{
				Limit:    jobStopLimit,
				MaxLimit: jobStopMaxLimit,
			},
		}
		request := &job.QueryRequest{
			Spec:        spec,
			SummaryOnly: true,
		}

		response, err := c.jobClient.Query(c.ctx, request)
		if err != nil {
			return err
		}

		for _, jobSummary := range response.GetResults() {
			printResponseJSON(jobSummary)
			jobsToStop = append(jobsToStop, jobSummary.GetId())
		}

		if !isForceStop && !askForConfirmation(jobStopConfirmationMessage) {
			return nil
		}
	}

	if len(jobsToStop) == 0 {
		fmt.Fprintf(tabWriter, "No matching job(s) found\n")
		tabWriter.Flush()
		return nil
	}

	for _, jobID := range jobsToStop {
		request := &task.StopRequest{
			JobId: jobID,
		}
		response, err := c.taskClient.Stop(c.ctx, request)

		if err != nil {
			stopFailedJobIDs = append(stopFailedJobIDs,
				jobID.GetValue())
			errs = multierr.Append(errs, err)
		} else {
			stopJobIDs = append(stopJobIDs, jobID.GetValue())
		}

		if showProgress {
			continue
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
				errs = multierr.Append(errs, err)
				continue
			}
			printTaskStopResponse(response, c.Debug)
		}
	}

	if showProgress {
		for _, jobID := range jobsToStop {
			if err := c.pollStatusWithTimeout(jobID); err != nil {
				errs = multierr.Append(errs, err)
			}
		}
	}

	fmt.Fprintf(tabWriter, "Stopping jobs: %v\n", stopJobIDs)
	if len(stopFailedJobIDs) != 0 {
		fmt.Fprintf(tabWriter, "Error stopping jobs: %v\n", stopFailedJobIDs)
	}
	tabWriter.Flush()
	return errs
}

// JobRestartAction is the action for restarting a job
func (c *Client) JobRestartAction(
	jobID string,
	resourceVersion uint64,
	instanceRanges []*task.InstanceRange,
	batchSize uint32,
) error {
	var response *job.RestartResponse
	var err error
	id := &peloton.JobID{
		Value: jobID,
	}

	err = c.retryUntilConcurrencyControlSucceeds(
		id,
		resourceVersion,
		func(resourceVersionParam uint64) error {
			request := &job.RestartRequest{
				Id:              id,
				Ranges:          instanceRanges,
				ResourceVersion: resourceVersionParam,
				RestartConfig: &job.RestartConfig{
					BatchSize: batchSize,
				},
			}
			response, err = c.jobClient.Restart(c.ctx, request)
			return err
		},
	)
	if err != nil {
		return err
	}

	printResponseJSON(response)
	return nil
}

// JobStartAction is the action for starting a job
func (c *Client) JobStartAction(
	jobID string,
	resourceVersion uint64,
	instanceRanges []*task.InstanceRange,
	batchSize uint32,
) error {
	var response *job.StartResponse
	var err error
	id := &peloton.JobID{
		Value: jobID,
	}
	err = c.retryUntilConcurrencyControlSucceeds(
		id,
		resourceVersion,
		func(resourceVersionParam uint64) error {
			request := &job.StartRequest{
				Id:              id,
				Ranges:          instanceRanges,
				ResourceVersion: resourceVersionParam,
				StartConfig: &job.StartConfig{
					BatchSize: batchSize,
				},
			}
			response, err = c.jobClient.Start(c.ctx, request)
			return err
		},
	)

	if err != nil {
		return err
	}

	printResponseJSON(response)
	return nil
}

// JobStopV1BetaAction is the action for stopping a job using new job API
func (c *Client) JobStopV1BetaAction(
	jobID string,
	resourceVersion uint64,
	instanceRanges []*task.InstanceRange,
	batchSize uint32,
) error {
	var response *job.StopResponse
	var err error
	id := &peloton.JobID{
		Value: jobID,
	}
	err = c.retryUntilConcurrencyControlSucceeds(
		id,
		resourceVersion,
		func(resourceVersionParam uint64) error {
			request := &job.StopRequest{
				Id:              id,
				Ranges:          instanceRanges,
				ResourceVersion: resourceVersionParam,
				StopConfig: &job.StopConfig{
					BatchSize: batchSize,
				},
			}

			response, err = c.jobClient.Stop(c.ctx, request)
			return err
		},
	)

	if err != nil {
		return err
	}

	printResponseJSON(response)
	return nil
}

func (c *Client) retryUntilConcurrencyControlSucceeds(
	id *peloton.JobID,
	resourceVersion uint64,
	fn func(
		resourceVersion uint64,
	) error,
) error {
	for {
		// first fetch the job runtime
		var jobGetRequest = &job.GetRequest{
			Id: id,
		}
		jobGetResponse, err := c.jobClient.Get(c.ctx, jobGetRequest)
		if err != nil {
			return err
		}

		jobRuntime := jobGetResponse.GetJobInfo().GetRuntime()
		if jobRuntime == nil {
			return fmt.Errorf("unable to find the job to restart")
		}

		if resourceVersion > 0 {
			if jobRuntime.GetConfigurationVersion() != resourceVersion {
				return fmt.Errorf(
					"invalid input resource version current %v provided %v",
					jobRuntime.GetConfigurationVersion(), resourceVersion)
			}
		}

		err = fn(jobRuntime.GetConfigurationVersion())
		if err != nil {
			if yarpcerrors.IsInvalidArgument(err) &&
				yarpcerrors.FromError(err).Message() == invalidVersionError &&
				resourceVersion == 0 {
				continue
			}
			return err
		}
		break
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
		fmt.Printf("Job %s stopped, total_tasks:%d terminated_tasks:%d\n",
			id.GetValue(), total, terminated)
		return nil
	}

	// init the bar to the total tasks
	bar := pb.Simple.
		Start(int(total)).
		SetTotal(int64(total)).
		SetWidth(150).
		SetRefreshRate(time.Second).
		Set("prefix", fmt.Sprintf("Job %s terminated/total: ", id.GetValue()))
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

func parsePelotonLabels(labels string) ([]*peloton.Label, error) {
	var pelotonLabels []*peloton.Label
	for _, l := range strings.Split(labels, labelSeparator) {
		labelVals := strings.Split(l, keyValSeparator)
		if len(labelVals) != 2 {
			fmt.Printf("Invalid label %v", l)
			return nil, errors.New("Invalid label" + l)
		}
		pelotonLabels = append(pelotonLabels, &peloton.Label{
			Key:   labelVals[0],
			Value: labelVals[1],
		})
	}
	return pelotonLabels, nil
}

// askForConfirmation uses Scanln to parse user input. A user must type in "yes" or "no" and
// then press enter. It has fuzzy matching, so "y", "Y", "yes", "YES", and "Yes" all count as
// confirmations. If the input is not recognized, it will ask again. The function does not return
// until it gets a valid response from the user.
func askForConfirmation(s string) bool {
	for {
		fmt.Printf("%s [y/n]: ", s)
		var response string
		_, err := fmt.Scanln(&response)
		if err != nil {
			return false
		}
		response = strings.ToLower(strings.TrimSpace(response))
		if response == "y" || response == "yes" {
			return true
		}
		if response == "n" || response == "no" {
			return false
		}
	}
}
