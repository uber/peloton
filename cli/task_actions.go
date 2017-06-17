package cli

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

const (
	taskListFormatHeader = "Instance\tName\tState\tStart Time\tRun Time\t" +
		"Host\tMessage\tReason\t\n"
	taskListFormatBody = "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n"
)

// SortedTaskInfoList makes TaskInfo implement sortable interface
type SortedTaskInfoList []*task.TaskInfo

func (a SortedTaskInfoList) Len() int           { return len(a) }
func (a SortedTaskInfoList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedTaskInfoList) Less(i, j int) bool { return a[i].InstanceId < a[j].InstanceId }

// TaskGetAction is the action to get a task instance
func (client *Client) TaskGetAction(jobID string, instanceID uint32) error {
	var request = &task.GetRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		InstanceId: instanceID,
	}
	response, err := client.taskClient.Get(client.ctx, request)
	if err != nil {
		return err
	}
	printTaskGetResponse(response, client.Debug)
	return nil
}

// TaskLogsGetAction is the action to get logs files for given job instance.
func (client *Client) TaskLogsGetAction(fileName string, jobID string, instanceID uint32) error {
	var request = &task.BrowseSandboxRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		InstanceId: instanceID,
	}
	response, err := client.taskClient.BrowseSandbox(client.ctx, request)
	if err != nil {
		return err
	}

	if response.GetError() != nil {
		return errors.New(response.Error.String())
	}

	var filePath string

	for _, path := range response.GetPaths() {
		if strings.HasSuffix(path, fileName) {
			filePath = path
		}
	}

	if len(filePath) == 0 {
		return fmt.Errorf(
			"filename:%s not found in sandbox files: %s",
			fileName,
			response.GetPaths())
	}

	logFileDownloadURL := fmt.Sprintf(
		"http://%s:%s/files/download?path=%s",
		response.GetHostname(),
		response.GetPort(),
		filePath)

	resp, err := http.Get(logFileDownloadURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	fmt.Printf("\n\n%s", body)

	return nil
}

// TaskListAction is the action to list tasks
func (client *Client) TaskListAction(jobID string, instanceRange *task.InstanceRange) error {
	var request = &task.ListRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		Range: instanceRange,
	}
	response, err := client.taskClient.List(client.ctx, request)

	if err != nil {
		return err
	}
	printTaskListResponse(response, client.Debug)
	return nil
}

// TaskStartAction is the action to start a task
func (client *Client) TaskStartAction(jobID string, instanceRanges []*task.InstanceRange) error {
	var request = &task.StartRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		Ranges: instanceRanges,
	}
	response, err := client.taskClient.Start(client.ctx, request)
	if err != nil {
		return err
	}
	printTaskStartResponse(response, client.Debug)
	return nil
}

// TaskStopAction is the action to stop a task
func (client *Client) TaskStopAction(jobID string, instanceRanges []*task.InstanceRange) error {
	var request = &task.StopRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		Ranges: instanceRanges,
	}
	response, err := client.taskClient.Stop(client.ctx, request)
	if err != nil {
		return err
	}
	printTaskStopResponse(response, client.Debug)
	return nil
}

// TaskRestartAction is the action to restart a task
func (client *Client) TaskRestartAction(jobID string, instanceRanges []*task.InstanceRange) error {
	var request = &task.RestartRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		Ranges: instanceRanges,
	}
	response, err := client.taskClient.Restart(client.ctx, request)
	if err != nil {
		return err
	}
	printTaskRestartResponse(response, client.Debug)
	return nil
}

// printTask print the single row output of the task
func printTask(t *task.TaskInfo) {
	cfg := t.GetConfig()
	runtime := t.GetRuntime()

	// Calcuate the start time and run time of the task
	startTimeStr := ""
	durationStr := ""
	startTime, err := time.Parse(time.RFC3339Nano, runtime.GetStartTime())
	if err == nil {
		startTimeStr = startTime.Format(time.RFC3339)
		completionTime, err := time.Parse(time.RFC3339Nano, runtime.GetCompletionTime())
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

	// Print the task record
	fmt.Fprintf(
		tabWriter,
		taskListFormatBody,
		t.GetInstanceId(),
		cfg.GetName(),
		runtime.GetState().String(),
		startTimeStr,
		durationStr,
		runtime.GetHost(),
		runtime.GetMessage(),
		runtime.GetReason(),
	)
}

func printTaskGetResponse(r *task.GetResponse, debug bool) {
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
			fmt.Fprintf(tabWriter, taskListFormatHeader)
			printTask(r.GetResult())
		} else {
			fmt.Fprintf(tabWriter, "Unexpected error, no results in response.\n")
		}
	}
	tabWriter.Flush()
}

func printTaskListResponse(r *task.ListResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.GetNotFound() != nil {
			fmt.Fprintf(tabWriter, "Job %s was not found: %s\n",
				r.NotFound.Id.Value, r.NotFound.Message)
		} else {
			fmt.Fprintf(tabWriter, taskListFormatHeader)

			// we want to show tasks in sorted order
			tasks := make(SortedTaskInfoList, len(r.GetResult().GetValue()))
			i := 0
			for _, k := range r.GetResult().GetValue() {
				tasks[i] = k
				i++
			}
			sort.Sort(tasks)

			for _, t := range tasks {
				printTask(t)
			}
		}
	}
	tabWriter.Flush()
}

func printTaskStartResponse(r *task.StartResponse, debug bool) {
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

func printTaskStopResponse(r *task.StopResponse, debug bool) {
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
					"Requested instances:%d of job %s is not within "+
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

func printTaskRestartResponse(r *task.RestartResponse, debug bool) {
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
