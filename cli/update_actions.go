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
	"fmt"
	"io/ioutil"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	updatesvc "github.com/uber/peloton/.gen/peloton/api/v0/update/svc"

	"go.uber.org/yarpc/yarpcerrors"
	"gopkg.in/yaml.v2"
)

const (
	updateListFormatHeader = "Update-ID\tState\tNumberTasksCompleted\t" +
		"NumberTasksFailed\tNumberTasksRemaining\n"
	updateListFormatBody = "%s\t%s\t%d\t%d\t%d\n"
	invalidVersionError  = "invalid job configuration version"
)

// isUpdateTerminated returns true if update is complete or abortee
func (c *Client) isUpdateTerminated(updateID *peloton.UpdateID) (bool, error) {
	var request = &updatesvc.GetUpdateRequest{
		UpdateId: updateID,
	}

	response, err := c.updateClient.GetUpdate(c.ctx, request)
	if err != nil {
		return false, err
	}

	switch response.GetUpdateInfo().GetStatus().GetState() {
	case update.State_SUCCEEDED, update.State_ABORTED,
		update.State_FAILED, update.State_ROLLED_BACK:
		return true, nil
	}
	return false, nil
}

// UpdateCreateAction will create a new job update.
func (c *Client) UpdateCreateAction(
	jobID string,
	cfg string,
	batchSize uint32,
	respoolPath string,
	configVersion uint64,
	override bool,
	maxInstanceAttempts uint32,
	maxFailureInstances uint32,
	updateRollbackOnFailure bool,
	updateStartInPausedState bool,
	opaqueData string,
	inPlace bool) error {
	var jobConfig job.JobConfig
	var response *updatesvc.CreateUpdateResponse

	// read the job configuration
	buffer, err := ioutil.ReadFile(cfg)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %v", cfg, err)
	}
	if err := yaml.Unmarshal(buffer, &jobConfig); err != nil {
		return fmt.Errorf("unable to parse file %s: %v", cfg, err)
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
	jobConfig.RespoolID = respoolID

	for {
		// first fetch the job runtime
		var jobGetRequest = &job.GetRequest{
			Id: &peloton.JobID{
				Value: jobID,
			},
		}
		jobGetResponse, err := c.jobClient.Get(c.ctx, jobGetRequest)
		if err != nil {
			return err
		}

		// check if there is another update going on
		jobRuntime := jobGetResponse.GetJobInfo().GetRuntime()
		if jobRuntime == nil {
			return fmt.Errorf("unable to find the job to update")
		}

		if configVersion > 0 {
			if jobRuntime.GetConfigurationVersion() != configVersion {
				return fmt.Errorf(
					"invalid input configuration version current %v provided %v",
					jobRuntime.GetConfigurationVersion(), configVersion)
			}
		}

		if jobRuntime.GetUpdateID() != nil &&
			len(jobRuntime.GetUpdateID().GetValue()) > 0 {
			terminal, err := c.isUpdateTerminated(jobRuntime.GetUpdateID())
			if err != nil {
				return err
			}

			if !terminal {
				if override {
					fmt.Fprintf(tabWriter, "going to override existing update: %v\n",
						jobRuntime.GetUpdateID().GetValue())
					tabWriter.Flush()
				} else {
					return fmt.Errorf(
						"cannot create a new update as another update is already running")
				}
			}
		}

		// set the configuration version
		jobConfig.ChangeLog = &peloton.ChangeLog{
			Version: jobRuntime.GetConfigurationVersion(),
		}

		var opaque *peloton.OpaqueData
		if len(opaqueData) > 0 {
			opaque = &peloton.OpaqueData{Data: opaqueData}
		}

		var request = &updatesvc.CreateUpdateRequest{
			JobId: &peloton.JobID{
				Value: jobID,
			},
			JobConfig: &jobConfig,
			UpdateConfig: &update.UpdateConfig{
				BatchSize:           batchSize,
				MaxInstanceAttempts: maxInstanceAttempts,
				MaxFailureInstances: maxFailureInstances,
				RollbackOnFailure:   updateRollbackOnFailure,
				StartPaused:         updateStartInPausedState,
				InPlace:             inPlace,
			},
			OpaqueData: opaque,
		}

		response, err = c.updateClient.CreateUpdate(c.ctx, request)
		if err != nil {
			if yarpcerrors.IsInvalidArgument(err) &&
				yarpcerrors.FromError(err).Message() == invalidVersionError {
				continue
			}
			return err
		}
		break
	}

	printUpdateCreateResponse(response, c.Debug)
	return nil
}

// UpdateGetAction gets the summary/full update information
func (c *Client) UpdateGetAction(updateID string) error {
	var request = &updatesvc.GetUpdateRequest{
		UpdateId: &peloton.UpdateID{
			Value: updateID,
		},
	}

	response, err := c.updateClient.GetUpdate(c.ctx, request)
	if err != nil {
		return err
	}

	printUpdateGetResponse(response, c.Debug)
	return nil
}

// UpdateListAction lists all actions of a job update
func (c *Client) UpdateListAction(jobID string) error {
	var request = &updatesvc.ListUpdatesRequest{
		JobID: &peloton.JobID{
			Value: jobID,
		},
	}

	response, err := c.updateClient.ListUpdates(c.ctx, request)
	if err != nil {
		return err
	}

	printUpdateListResponse(response, c.Debug)
	return nil
}

// UpdateGetCacheAction fetches the information stored in the cache for the update
func (c *Client) UpdateGetCacheAction(updateID string) error {
	var request = &updatesvc.GetUpdateCacheRequest{
		UpdateId: &peloton.UpdateID{
			Value: updateID,
		},
	}

	response, err := c.updateClient.GetUpdateCache(c.ctx, request)
	if err != nil {
		return err
	}

	defer tabWriter.Flush()
	printResponseJSON(response)
	return nil
}

// UpdateAbortAction aborts a given update
func (c *Client) UpdateAbortAction(updateID string, opaqueData string) error {
	var opaque *peloton.OpaqueData
	if len(opaqueData) > 0 {
		opaque = &peloton.OpaqueData{Data: opaqueData}
	}

	var request = &updatesvc.AbortUpdateRequest{
		UpdateId: &peloton.UpdateID{
			Value: updateID,
		},
		OpaqueData: opaque,
	}

	_, err := c.updateClient.AbortUpdate(c.ctx, request)
	if err != nil {
		return err
	}
	return nil
}

// UpdateResumeAction resumes a given update
func (c *Client) UpdateResumeAction(updateID string, opaqueData string) error {
	var opaque *peloton.OpaqueData
	if len(opaqueData) > 0 {
		opaque = &peloton.OpaqueData{Data: opaqueData}
	}

	var request = &updatesvc.ResumeUpdateRequest{
		UpdateId: &peloton.UpdateID{
			Value: updateID,
		},
		OpaqueData: opaque,
	}

	_, err := c.updateClient.ResumeUpdate(c.ctx, request)
	if err != nil {
		return err
	}
	return nil
}

// UpdatePauseAction pauses a given update
func (c *Client) UpdatePauseAction(updateID string, opaqueData string) error {
	var opaque *peloton.OpaqueData
	if len(opaqueData) > 0 {
		opaque = &peloton.OpaqueData{Data: opaqueData}
	}

	var request = &updatesvc.PauseUpdateRequest{
		UpdateId: &peloton.UpdateID{
			Value: updateID,
		},
		OpaqueData: opaque,
	}

	_, err := c.updateClient.PauseUpdate(c.ctx, request)
	if err != nil {
		return err
	}
	return nil
}

// printUpdateCreateResponse prints the update identifier returned in the
// create job update response.
func printUpdateCreateResponse(resp *updatesvc.CreateUpdateResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(resp)
		return
	}

	if resp.GetUpdateID() != nil {
		fmt.Fprintf(tabWriter, "Job update %s created\n",
			resp.GetUpdateID().GetValue())
	}
	return
}

// printUpdate prints the update status information for a single update
func printUpdate(u *update.UpdateInfo) {
	status := u.GetStatus()
	fmt.Fprintf(
		tabWriter,
		updateListFormatBody,
		u.GetUpdateId().GetValue(),
		status.GetState().String(),
		status.GetNumTasksDone(),
		status.GetNumTasksFailed(),
		status.GetNumTasksRemaining(),
	)
}

// printUpdateGetResponse prints the update get response
func printUpdateGetResponse(resp *updatesvc.GetUpdateResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(resp)
		return
	}

	if resp.GetUpdateInfo() == nil {
		return
	}

	fmt.Fprint(tabWriter, updateListFormatHeader)
	printUpdate(resp.GetUpdateInfo())
	return
}

// printUpdateListResponse prints the update list response
func printUpdateListResponse(resp *updatesvc.ListUpdatesResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(resp)
		return
	}

	if len(resp.GetUpdateInfo()) == 0 {
		return
	}

	fmt.Fprint(tabWriter, updateListFormatHeader)
	for _, update := range resp.GetUpdateInfo() {
		printUpdate(update)
	}
	return
}
