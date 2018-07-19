package cli

import (
	"fmt"
	"io/ioutil"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	updatesvc "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update/svc"

	"gopkg.in/yaml.v2"
)

const (
	updateListFormatHeader = "Update-ID\tState\tNumberTasksCompleted\t" +
		"NumberTasksRemaining\n"
	updateListFormatBody = "%s\t%s\t%d\t%d\n"
)

// UpdateCreateAction will create a new job update.
func (c *Client) UpdateCreateAction(
	jobID string, cfg string, batchSize uint32) error {
	var jobConfig job.JobConfig

	buffer, err := ioutil.ReadFile(cfg)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %v", cfg, err)
	}
	if err := yaml.Unmarshal(buffer, &jobConfig); err != nil {
		return fmt.Errorf("unable to parse file %s: %v", cfg, err)
	}

	var request = &updatesvc.CreateUpdateRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		JobConfig: &jobConfig,
		UpdateConfig: &update.UpdateConfig{
			BatchSize: batchSize,
		},
	}

	response, err := c.updateClient.CreateUpdate(c.ctx, request)
	if err != nil {
		return err
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
func (c *Client) UpdateAbortAction(updateID string) error {
	var request = &updatesvc.AbortUpdateRequest{
		UpdateId: &peloton.UpdateID{
			Value: updateID,
		},
	}

	_, err := c.updateClient.AbortUpdate(c.ctx, request)
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
