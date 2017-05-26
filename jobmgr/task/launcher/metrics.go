package launcher

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of task launcher.
type Metrics struct {
	TaskLaunch     tally.Counter
	TaskLaunchFail tally.Counter

	GetPlacement              tally.Counter
	GetPlacementFail          tally.Counter
	GetDBTaskInfo             tally.Timer
	LauncherGoRoutines        tally.Counter
	GetPlacementsCallDuration tally.Timer
	LaunchTasksCallDuration   tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	taskSuccessScope := scope.Tagged(map[string]string{"type": "success"})
	taskFailScope := scope.Tagged(map[string]string{"type": "fail"})
	taskAPIScope := scope.SubScope("task_api")
	functionCallScope := scope.SubScope("functioncall")
	getPlacementScope := scope.SubScope("get_placement")
	launchTaskScope := scope.SubScope("launch_tasks")

	return &Metrics{
		TaskLaunch:                taskSuccessScope.Counter("launch"),
		TaskLaunchFail:            taskFailScope.Counter("launch"),
		GetPlacement:              taskAPIScope.Counter("get_placement"),
		GetPlacementFail:          taskFailScope.Counter("get_placement"),
		GetDBTaskInfo:             functionCallScope.Timer("get_taskinfo"),
		LauncherGoRoutines:        getPlacementScope.Counter("go_routines"),
		GetPlacementsCallDuration: getPlacementScope.Timer("call_duration"),
		LaunchTasksCallDuration:   launchTaskScope.Timer("call_duration"),
	}
}
