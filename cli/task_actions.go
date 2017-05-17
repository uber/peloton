package cli

import (
	"fmt"
	"sort"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"go.uber.org/yarpc"
)

const (
	taskListFormatHeader = "Instance\tJob\tCPU Limit\tMem Limit\tDisk Limit\tState\tGoalState\tStarted At" +
		"\tTask ID\tHost\tMessage\tReason\t\n"
	taskListFormatBody = "%d\t%s\t%.1f\t%.0f MB\t%.0f MB\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n"
)

// SortedTaskInfoList makes TaskInfo implement sortable interface
type SortedTaskInfoList []*task.TaskInfo

func (a SortedTaskInfoList) Len() int           { return len(a) }
func (a SortedTaskInfoList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedTaskInfoList) Less(i, j int) bool { return a[i].InstanceId < a[j].InstanceId }

// TaskGetAction is the action to get a task instance
func (client *Client) TaskGetAction(jobName string, instanceID uint32) error {
	var response task.GetResponse
	var request = &task.GetRequest{
		JobId: &peloton.JobID{
			Value: jobName,
		},
		InstanceId: instanceID,
	}
	_, err := client.jobClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("TaskManager.Get"),
		request,
		&response,
	)
	if err != nil {
		return err
	}
	printTaskGetResponse(response, client.Debug)
	return nil
}

// TaskListAction is the action to list tasks
func (client *Client) TaskListAction(jobName string, instanceRange *task.InstanceRange) error {
	var request = &task.ListRequest{
		JobId: &peloton.JobID{
			Value: jobName,
		},
		Range: instanceRange,
	}
	var response task.ListResponse
	_, err := client.jobClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("TaskManager.List"),
		request,
		&response,
	)

	if err != nil {
		return err
	}
	printTaskListResponse(response, client.Debug)
	return nil
}

// TaskStartAction is the action to start a task
func (client *Client) TaskStartAction(jobName string, instanceRanges []*task.InstanceRange) error {
	var response task.StartResponse
	var request = &task.StartRequest{
		JobId: &peloton.JobID{
			Value: jobName,
		},
		Ranges: instanceRanges,
	}
	_, err := client.jobClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("TaskManager.Start"),
		request,
		&response,
	)
	if err != nil {
		return err
	}
	printTaskStartResponse(response, client.Debug)
	return nil
}

// TaskStopAction is the action to stop a task
func (client *Client) TaskStopAction(jobName string, instanceRanges []*task.InstanceRange) error {
	var response task.StopResponse
	var request = &task.StopRequest{
		JobId: &peloton.JobID{
			Value: jobName,
		},
		Ranges: instanceRanges,
	}
	_, err := client.jobClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("TaskManager.Stop"),
		request,
		&response,
	)
	if err != nil {
		return err
	}
	printTaskStopResponse(response, client.Debug)
	return nil
}

// TaskRestartAction is the action to restart a task
func (client *Client) TaskRestartAction(jobName string, instanceRanges []*task.InstanceRange) error {
	var response task.RestartResponse
	var request = &task.RestartRequest{
		JobId: &peloton.JobID{
			Value: jobName,
		},
		Ranges: instanceRanges,
	}
	_, err := client.jobClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("TaskManager.Restart"),
		request,
		&response,
	)
	if err != nil {
		return err
	}
	printTaskRestartResponse(response, client.Debug)
	return nil
}

func printTaskGetResponse(r task.GetResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.GetNotFound() != nil {
			fmt.Fprintf(tabWriter, "Job %s was not found: %s\n",
				r.NotFound.Id.Value, r.NotFound.Message)
		} else if r.GetOutOfRange() != nil {
			fmt.Fprintf(tabWriter,
				"Requested instance of job %s is not within the range of valid "+
					"instances (0...%d)\n",
				r.OutOfRange.JobId.Value, r.OutOfRange.InstanceCount)
		} else if r.GetResult() != nil {
			cfg := r.Result.Config
			rt := r.Result.Runtime
			fmt.Fprintf(tabWriter, taskListFormatHeader)
			fmt.Fprintf(
				tabWriter,
				taskListFormatBody,
				r.Result.InstanceId,
				cfg.Name,
				cfg.Resource.CpuLimit,
				cfg.Resource.MemLimitMb,
				cfg.Resource.DiskLimitMb,
				rt.State.String(),
				rt.GoalState.String(),
				rt.StartedAt,
				rt.GetTaskId().GetValue(),
				rt.Host,
				rt.Message,
				rt.Reason)
		} else {
			fmt.Fprintf(tabWriter, "Unexpected error, no results in response.\n")
		}
	}
	tabWriter.Flush()
}

func printTaskListResponse(r task.ListResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.GetNotFound() != nil {
			fmt.Fprintf(tabWriter, "Job %s was not found: %s\n",
				r.NotFound.Id.Value, r.NotFound.Message)
		} else {
			fmt.Fprintf(tabWriter, taskListFormatHeader)

			// we want to show tasks in sorted order
			tasks := make(SortedTaskInfoList, len(r.Result.Value))
			i := 0
			for _, k := range r.Result.Value {
				tasks[i] = k
				i++
			}
			sort.Sort(tasks)

			for _, t := range tasks {
				cfg := t.Config
				rt := t.Runtime
				fmt.Fprintf(
					tabWriter,
					taskListFormatBody,
					t.InstanceId,
					cfg.Name,
					cfg.Resource.CpuLimit,
					cfg.Resource.MemLimitMb,
					cfg.Resource.DiskLimitMb,
					rt.State.String(),
					rt.GoalState.String(),
					rt.StartedAt,
					rt.GetTaskId().GetValue(),
					rt.Host,
					rt.Message,
					rt.Reason)
			}
		}
	}
	tabWriter.Flush()
}

func printTaskStartResponse(r task.StartResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.GetNotFound() != nil {
			fmt.Fprintf(tabWriter, "Job %s was not found: %s\n", r.NotFound.Id.Value, r.NotFound.Message)
		} else if r.GetOutOfRange() != nil {
			fmt.Fprintf(tabWriter, "Requested instance of job %s is not within "+
				"the range of valid instances (0...%d)\n",
				r.OutOfRange.JobId.Value, r.OutOfRange.InstanceCount)
		} else {
			fmt.Fprintf(tabWriter, "Job started\n")
		}
	}
	tabWriter.Flush()
}

func printTaskStopResponse(r task.StopResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.GetError() != nil {
			if r.GetError().GetNotFound() != nil {
				fmt.Fprintf(
					tabWriter,
					"Job %s was not found: %s\n",
					r.GetError().GetNotFound().GetId().GetValue(),
					r.GetError().GetNotFound().GetMessage(),
				)
			} else if r.GetError().GetOutOfRange() != nil {
				fmt.Fprintf(
					tabWriter,
					"Requested instances:%s of job %s is not within "+
						"the range of valid instances (0...%d)\n",
					r.GetInvalidInstanceIds(),
					r.GetError().GetOutOfRange().GetJobId().GetValue(),
					r.GetError().GetOutOfRange().GetInstanceCount(),
				)
			} else if r.GetError().GetUpdateError() != nil {
				fmt.Fprintf(
					tabWriter,
					"Tasks stop goalstate update in DB got error: %s\n",
					r.GetError().GetUpdateError(),
				)
			}
		} else {
			fmt.Fprintf(
				tabWriter,
				"Tasks stopped successfully for instances: %v and "+
					"invalid instances are: %v",
				r.GetStoppedInstanceIds(),
				r.GetInvalidInstanceIds(),
			)
		}
	}
	tabWriter.Flush()
}

func printTaskRestartResponse(r task.RestartResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.GetNotFound() != nil {
			fmt.Fprintf(tabWriter, "Job %s was not found: %s\n", r.NotFound.Id.Value, r.NotFound.Message)
		} else if r.GetOutOfRange() != nil {
			fmt.Fprintf(tabWriter, "Requested instance of job %s is not within the range of valid instances (0...%d)\n", r.OutOfRange.JobId.Value, r.OutOfRange.InstanceCount)
		} else {
			fmt.Fprintf(tabWriter, "Job restarted\n")
		}
	}
	tabWriter.Flush()
}
