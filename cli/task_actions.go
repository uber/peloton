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
	"code.uber.internal/infra/peloton/.gen/peloton/api/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

const (
	taskListFormatHeader = "Instance\tName\tState\tStart Time\tRun Time\t" +
		"Host\tMessage\tReason\t\n"
	taskListFormatBody     = "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n"
	taskEventsFormatHeader = "Mesos Task Id\tState\tEvent Time\tHost\tMessage\tReason\t\n"
	taskEventsFormatBody   = "%s\t%s\t%s\t%s\t%s\t%s\t\n"
)

// sortedTaskInfoList makes TaskInfo implement sortable interface
type sortedTaskInfoList []*task.TaskInfo

func (a sortedTaskInfoList) Len() int           { return len(a) }
func (a sortedTaskInfoList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortedTaskInfoList) Less(i, j int) bool { return a[i].InstanceId < a[j].InstanceId }

// TaskGetAction is the action to get a task instance
func (c *Client) TaskGetAction(jobID string, instanceID uint32) error {
	var request = &task.GetRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		InstanceId: instanceID,
	}
	response, err := c.taskClient.Get(c.ctx, request)
	if err != nil {
		return err
	}
	printTaskGetResponse(response, c.Debug)
	return nil
}

// TaskLogsGetAction is the action to get logs files for given job instance.
func (c *Client) TaskLogsGetAction(fileName string, jobID string, instanceID uint32, taskID string) error {
	var request = &task.BrowseSandboxRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		InstanceId: instanceID,
		TaskId:     taskID,
	}
	response, err := c.taskClient.BrowseSandbox(c.ctx, request)
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

// TaskGetEventsAction is the action to get a task instance
func (c *Client) TaskGetEventsAction(jobID string, instanceID uint32) error {
	var request = &task.GetEventsRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		InstanceId: instanceID,
	}
	response, err := c.taskClient.GetEvents(c.ctx, request)
	if err != nil {
		return err
	}
	printTaskGetEventsResponse(response, c.Debug)
	return nil
}

// TaskListAction is the action to list tasks
func (c *Client) TaskListAction(jobID string, instanceRange *task.InstanceRange) error {
	var request = &task.ListRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		Range: instanceRange,
	}
	response, err := c.taskClient.List(c.ctx, request)

	if err != nil {
		return err
	}
	printTaskListResponse(response, c.Debug)
	return nil
}

// TaskQueryAction is the action to query task
func (c *Client) TaskQueryAction(
	jobID string,
	states string,
	names string,
	hosts string,
	limit uint32,
	offset uint32,
	sortBy string,
	sortOrder string) error {
	var taskStates []task.TaskState
	var taskNames, taskHosts []string
	for _, k := range strings.Split(states, labelSeparator) {
		if k != "" {
			taskStates = append(taskStates, task.TaskState(task.TaskState_value[k]))
		}
	}

	for _, host := range strings.Split(hosts, labelSeparator) {
		if host != "" {
			taskHosts = append(taskHosts, host)
		}
	}

	for _, name := range strings.Split(names, labelSeparator) {
		if name != "" {
			taskNames = append(taskNames, name)
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

	var request = &task.QueryRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		Spec: &task.QuerySpec{
			TaskStates: taskStates,
			Names:      taskNames,
			Hosts:      taskHosts,
			Pagination: &query.PaginationSpec{
				Limit:   limit,
				Offset:  offset,
				OrderBy: sort,
			},
		},
	}
	response, err := c.taskClient.Query(c.ctx, request)

	if err != nil {
		return err
	}
	printTaskQueryResponse(response, c.Debug)
	return nil
}

// TaskRefreshAction calls task refresh API
func (c *Client) TaskRefreshAction(jobID string, instanceRange *task.InstanceRange) error {
	var request = &task.RefreshRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		Range: instanceRange,
	}
	_, err := c.taskClient.Refresh(c.ctx, request)
	return err
}

// TaskStartAction is the action to start a task
func (c *Client) TaskStartAction(jobID string, instanceRanges []*task.InstanceRange) error {
	var request = &task.StartRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		Ranges: instanceRanges,
	}
	response, err := c.taskClient.Start(c.ctx, request)
	if err != nil {
		return err
	}
	printTaskStartResponse(response, c.Debug)
	return nil
}

// TaskStopAction is the action to stop a task
func (c *Client) TaskStopAction(jobID string,
	instanceRanges []*task.InstanceRange) error {

	id := &peloton.JobID{
		Value: jobID,
	}

	var request = &task.StopRequest{
		JobId:  id,
		Ranges: instanceRanges,
	}
	response, err := c.taskClient.Stop(c.ctx, request)
	if err != nil {
		return err
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

// TaskRestartAction is the action to restart a task
func (c *Client) TaskRestartAction(jobID string, instanceRanges []*task.InstanceRange) error {
	var request = &task.RestartRequest{
		JobId: &peloton.JobID{
			Value: jobID,
		},
		Ranges: instanceRanges,
	}
	response, err := c.taskClient.Restart(c.ctx, request)
	if err != nil {
		return err
	}
	printTaskRestartResponse(response, c.Debug)
	return nil
}

// printTask print the single row output of the task
func printTask(t *task.TaskInfo) {
	cfg := t.GetConfig()
	runtime := t.GetRuntime()

	// Calculate the start time and run time of the task
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

// printTaskEvents print the list of events for the task
func printTaskEvents(eventsList []*task.GetEventsResponse_Events) {
	// Print the task events
	for _, events := range eventsList {
		for _, event := range events.GetEvent() {
			fmt.Fprintf(
				tabWriter,
				taskEventsFormatBody,
				event.GetTaskId().GetValue(),
				event.GetState(),
				event.GetTimestamp(),
				event.GetHostname(),
				event.GetMessage(),
				event.GetReason(),
			)
		}
	}
}

func printTaskGetResponse(r *task.GetResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(r)
		return
	}

	if r.GetNotFound() != nil {
		fmt.Fprintf(tabWriter, "Job %s was not found: %s\n",
			r.NotFound.Id.Value, r.NotFound.Message)
		return
	}

	if r.GetOutOfRange() != nil {
		fmt.Fprintf(tabWriter,
			"Requested instance of job %s is not within the range of valid "+
				"instances (0...%d)\n",
			r.OutOfRange.JobId.Value, r.OutOfRange.InstanceCount)
		return
	}

	if r.GetResult() != nil {
		fmt.Fprint(tabWriter, taskListFormatHeader)
		printTask(r.GetResult())
		return
	}
	fmt.Fprint(tabWriter, "Unexpected error, no results in response.\n")
}

func printTaskGetEventsResponse(r *task.GetEventsResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(r)
		return
	}

	err := r.GetError()
	if err == nil {
		fmt.Fprint(tabWriter, taskEventsFormatHeader)
		printTaskEvents(r.GetResult())
		return
	}

	if err.GetEventError() != nil {
		fmt.Fprintf(tabWriter,
			"Got event error: %s\n", err.GetEventError().GetMessage())
		return
	}

	fmt.Fprintf(tabWriter, "Unexpected error %v\n", err)
}

func printTaskListResponse(r *task.ListResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(r)
		return
	}

	if r.GetNotFound() != nil {
		fmt.Fprintf(tabWriter, "Job %s was not found: %s\n",
			r.NotFound.Id.Value, r.NotFound.Message)
		return
	}

	fmt.Fprint(tabWriter, taskListFormatHeader)
	// we want to show tasks in sorted order
	tasks := make(sortedTaskInfoList, len(r.GetResult().GetValue()))
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

func printTaskQueryResponse(r *task.QueryResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(r)
		return
	}

	if r.GetError().GetNotFound() != nil {
		fmt.Fprintf(tabWriter, "Job %s was not found: %s\n",
			r.Error.NotFound.Id.Value, r.Error.NotFound.Message)
		return
	}

	if len(r.GetRecords()) == 0 {
		fmt.Fprint(tabWriter, "No tasks found\n")
		return
	}

	fmt.Fprint(tabWriter, taskListFormatHeader)
	for _, t := range r.GetRecords() {
		printTask(t)
	}
}

func printTaskStartResponse(r *task.StartResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(r)
		return
	}
	respError := r.GetError()
	if respError == nil {
		fmt.Fprintf(
			tabWriter,
			"Tasks started successfully for instances: %v and "+
				"failed instances are: %v\n",
			r.GetStartedInstanceIds(),
			r.GetInvalidInstanceIds(),
		)
		return
	}
	if respError.GetNotFound() != nil {
		fmt.Fprintf(
			tabWriter,
			"Job %s was not found: %s\n",
			respError.GetNotFound().GetId().GetValue(),
			respError.GetNotFound().GetMessage(),
		)
		return
	}

	if respError.GetOutOfRange() != nil {
		fmt.Fprintf(
			tabWriter,
			"Requested instances:%d of job %s is not within "+
				"the range of valid instances (0...%d)\n",
			r.GetInvalidInstanceIds(),
			respError.GetOutOfRange().GetJobId().GetValue(),
			respError.GetOutOfRange().GetInstanceCount(),
		)
		return
	}

	if r.GetError().GetFailure() != nil {
		fmt.Fprintf(
			tabWriter,
			"Tasks stop goalstate update in DB got error: %s\n",
			respError.GetFailure().GetMessage(),
		)
	}
}

func printTaskStopResponse(r *task.StopResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(r)
		return
	}

	respError := r.GetError()
	if respError == nil {
		if len(r.GetInvalidInstanceIds()) == 0 {
			fmt.Fprintf(
				tabWriter, "Successfully signalled %v tasks for stopping\n",
				len(r.GetStoppedInstanceIds()))
			return
		}

		fmt.Fprintf(
			tabWriter,
			"Some tasks failed to stop, failed_instance_ids:%v",
			r.GetInvalidInstanceIds(),
		)
		return
	}

	if respError.GetNotFound() != nil {
		fmt.Fprintf(
			tabWriter,
			"Job %s was not found: %s\n",
			respError.GetNotFound().GetId().GetValue(),
			respError.GetNotFound().GetMessage(),
		)
		return
	}

	if respError.GetOutOfRange() != nil {
		fmt.Fprintf(
			tabWriter,
			"Requested instances:%d of job %s is not within "+
				"the range of valid instances (0...%d)\n",
			r.GetInvalidInstanceIds(),
			respError.GetOutOfRange().GetJobId().GetValue(),
			respError.GetOutOfRange().GetInstanceCount(),
		)
		return
	}

	if respError.GetUpdateError() != nil {
		fmt.Fprintf(
			tabWriter,
			"Tasks stop failed with error: %s\n",
			respError.GetUpdateError(),
		)
	}
}

func printTaskRestartResponse(r *task.RestartResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(r)
		return
	}

	if r.GetNotFound() != nil {
		fmt.Fprintf(tabWriter, "Job %s was not found: %s\n",
			r.NotFound.Id.Value, r.NotFound.Message)
		return
	}

	if r.GetOutOfRange() != nil {
		fmt.Fprintf(tabWriter, "Requested instance of job %s is not "+
			"within the range of valid instances (0...%d)\n",
			r.OutOfRange.JobId.Value, r.OutOfRange.InstanceCount)
		return
	}

	fmt.Fprint(tabWriter, "Job restarted\n")
}
