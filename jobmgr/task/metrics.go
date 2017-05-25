package task

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track
// internal state of the job service
type Metrics struct {
	TaskAPIGet      tally.Counter
	TaskGet         tally.Counter
	TaskGetFail     tally.Counter
	TaskCreate      tally.Counter
	TaskCreateFail  tally.Counter
	TaskAPIList     tally.Counter
	TaskList        tally.Counter
	TaskListFail    tally.Counter
	TaskAPIStart    tally.Counter
	TaskStart       tally.Counter
	TaskStartFail   tally.Counter
	TaskAPIStop     tally.Counter
	TaskStop        tally.Counter
	TaskStopFail    tally.Counter
	TaskAPIRestart  tally.Counter
	TaskRestart     tally.Counter
	TaskRestartFail tally.Counter
	TaskLaunch      tally.Counter
	TaskLaunchFail  tally.Counter
	TaskAPIQuery    tally.Counter
	TaskQuery       tally.Counter
	TaskQueryFail   tally.Counter

	TaskAPIListLogs  tally.Counter
	TaskListLogs     tally.Counter
	TaskListLogsFail tally.Counter

	GetPlacement              tally.Counter
	GetPlacementFail          tally.Counter
	GetDBTaskInfo             tally.Timer
	LauncherGoRoutines        tally.Counter
	GetPlacementsCallDuration tally.Timer
	LaunchTasksCallDuration   tally.Timer

	RetryFailedTasksTotal tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	taskSuccessScope := scope.Tagged(map[string]string{"type": "success"})
	taskFailScope := scope.Tagged(map[string]string{"type": "fail"})
	taskAPIScope := scope.SubScope("api")
	functionCallScope := scope.SubScope("functioncall")
	getPlacementScope := scope.SubScope("get_placement")
	launchTaskScope := scope.SubScope("launch_tasks")
	retryTaskScope := scope.SubScope("retry_tasks")

	return &Metrics{
		TaskAPIGet:                taskAPIScope.Counter("get"),
		TaskGet:                   taskSuccessScope.Counter("get"),
		TaskGetFail:               taskFailScope.Counter("get"),
		TaskCreate:                taskSuccessScope.Counter("create"),
		TaskCreateFail:            taskFailScope.Counter("create"),
		TaskAPIList:               taskAPIScope.Counter("list"),
		TaskList:                  taskSuccessScope.Counter("list"),
		TaskListFail:              taskFailScope.Counter("list"),
		TaskAPIStart:              taskAPIScope.Counter("start"),
		TaskStart:                 taskSuccessScope.Counter("start"),
		TaskStartFail:             taskFailScope.Counter("start"),
		TaskAPIStop:               taskAPIScope.Counter("stop"),
		TaskStop:                  taskSuccessScope.Counter("stop"),
		TaskStopFail:              taskFailScope.Counter("stop"),
		TaskAPIRestart:            taskAPIScope.Counter("restart"),
		TaskRestart:               taskSuccessScope.Counter("restart"),
		TaskRestartFail:           taskFailScope.Counter("restart"),
		TaskLaunch:                taskSuccessScope.Counter("launch"),
		TaskLaunchFail:            taskFailScope.Counter("launch"),
		TaskAPIQuery:              taskAPIScope.Counter("query"),
		TaskQuery:                 taskSuccessScope.Counter("query"),
		TaskQueryFail:             taskFailScope.Counter("query"),
		TaskAPIListLogs:           taskAPIScope.Counter("list_logs"),
		TaskListLogs:              taskSuccessScope.Counter("list_logs"),
		TaskListLogsFail:          taskFailScope.Counter("list_logs"),
		GetPlacement:              taskAPIScope.Counter("get_placement"),
		GetPlacementFail:          taskFailScope.Counter("get_placement"),
		GetDBTaskInfo:             functionCallScope.Timer("get_taskinfo"),
		LauncherGoRoutines:        getPlacementScope.Counter("go_routines"),
		GetPlacementsCallDuration: getPlacementScope.Timer("call_duration"),
		LaunchTasksCallDuration:   launchTaskScope.Timer("call_duration"),
		RetryFailedTasksTotal:     retryTaskScope.Counter("failed_total"),
	}
}
