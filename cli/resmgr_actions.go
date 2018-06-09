package cli

import (
	"fmt"
	"strings"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
)

const (
	activeTaskListFormatHeader = "TaskID\tState\tReason\tLast Update Time\n"
	activeTaskListFormatBody   = "%s\t%s\t%s\t%s\n"
)

// ResMgrGetActiveTasks fetches the active tasks from resource manager.
func (c *Client) ResMgrGetActiveTasks(jobID string, respoolID string, states string) error {
	var apiStates []string
	for _, k := range strings.Split(states, labelSeparator) {
		if k != "" {
			apiStates = append(apiStates, k)
		}
	}

	var request = &resmgrsvc.GetActiveTasksRequest{
		JobID:     jobID,
		RespoolID: respoolID,
		States:    apiStates,
	}
	resp, err := c.resMgrClient.GetActiveTasks(c.ctx, request)
	if err != nil {
		return err
	}
	printActiveTasksResponse(resp, c.Debug)
	return nil
}

// ResMgrGetPendingTasks fetches the pending tasks from resource manager.
func (c *Client) ResMgrGetPendingTasks(respoolID string, limit uint32) error {

	var request = &resmgrsvc.GetPendingTasksRequest{
		RespoolID: &peloton.ResourcePoolID{Value: respoolID},
		Limit:     limit,
	}
	resp, err := c.resMgrClient.GetPendingTasks(c.ctx, request)
	if err != nil {
		return err
	}
	printPendingTasksResponse(resp, c.Debug)
	return nil
}

func printActiveTasksResponse(r *resmgrsvc.GetActiveTasksResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.GetError() != nil {
			fmt.Fprintf(tabWriter, r.GetError().GetMessage())
		} else {
			fmt.Fprint(tabWriter, activeTaskListFormatHeader)
			for _, taskEntry := range r.GetTasksByState() {
				for _, task := range taskEntry.GetTaskEntry() {
					fmt.Fprintf(
						tabWriter,
						activeTaskListFormatBody,
						task.GetTaskID(),
						task.GetTaskState(),
						task.GetReason(),
						task.GetLastUpdateTime())
				}
			}
		}
	}
	tabWriter.Flush()
}

func printPendingTasksResponse(r *resmgrsvc.GetPendingTasksResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		out, err := marshallResponse("yaml", r)
		if err == nil {
			fmt.Printf("%v\n", string(out))
		} else {
			fmt.Fprint(tabWriter, "Unable to marshall response\n")
		}
	}
	tabWriter.Flush()
}
