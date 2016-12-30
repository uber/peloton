package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"gopkg.in/alecthomas/kingpin.v2"
	pt "peloton/task"
)

var (
	// version of the peloton client. will be set by Makefile
	version   string
	client    *Client
	tabWriter = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight|tabwriter.Debug)

	app = kingpin.New("peloton", "Peloton CLI for interacting with peloton-master")
	// Global CLI flags

	debug        = app.Flag("debug", "enable debug mode (print full json responses)").Short('d').Default("false").Bool()
	frameworkURL = app.Flag("master", "name of the master address to use (http/tchannel) (set $MASTER_URL to override)").
			Short('m').Default("http://localhost:5289").OverrideDefaultFromEnvar("MASTER_URL").URL()
	timeout = app.Flag("timeout", "default RPC timeout (set $TIMEOUT to override)").
		Default("2s").OverrideDefaultFromEnvar("TIMEOUT").Duration()

	// Top level job command
	job = app.Command("job", "manage jobs")

	jobCreate       = job.Command("create", "create a job")
	jobCreateName   = jobCreate.Arg("job", "job identifier").Required().String()
	jobCreateConfig = jobCreate.Arg("config", "YAML job configuration").Required().ExistingFile()

	jobDelete     = job.Command("cancel", "cancel a job").Alias("delete")
	jobDeleteName = jobDelete.Arg("job", "job identifier").Required().String()

	jobGet     = job.Command("get", "get a job")
	jobGetName = jobGet.Arg("job", "job identifier").Required().String()

	// Top level task command
	task = app.Command("task", "manage tasks")

	taskGet           = task.Command("get", "show task status")
	taskGetJobName    = taskGet.Arg("job", "job identifier").Required().String()
	taskGetInstanceID = taskGet.Arg("instance", "job instance id").Required().Uint32()

	taskList              = task.Command("list", "show tasks of a job")
	taskListJobName       = taskList.Arg("job", "job identifier").Required().String()
	taskListInstanceRange = taskRangeFlag(taskList.Flag("range", "show range of instances (from:to syntax)").Default(":").Short('r'))

	taskStart               = task.Command("start", "start a task")
	taskStartJobName        = taskStart.Arg("job", "job identifier").Required().String()
	taskStartInstanceRanges = taskRangeListFlag(taskStart.Flag("range", "start range of instances (specify multiple times) (from:to syntax, default ALL)").Default(":").Short('r'))

	taskStop               = task.Command("stop", "stop a task")
	taskStopJobName        = taskStop.Arg("job", "job identifier").Required().String()
	taskStopInstanceRanges = taskRangeListFlag(taskStop.Flag("range", "stop range of instances (specify multiple times) (from:to syntax, default ALL)").Default(":").Short('r'))

	taskRestart               = task.Command("restart", "restart a task")
	taskRestartJobName        = taskRestart.Arg("job", "job identifier").Required().String()
	taskRestartInstanceRanges = taskRangeListFlag(taskRestart.Flag("range", "restart range of instances (specify multiple times) (from:to syntax, default ALL)").Default(":").Short('r'))
)

// TaskRangeValue allows us to define a new target type for kingpin to allow specifying ranges of tasks with from:to syntax as a TaskRangeFlag
type TaskRangeValue pt.InstanceRange

// TaskRangeListValue allows for collecting kingpin Values with a cumulative flag parser for instance ranges
type TaskRangeListValue struct {
	s []*pt.InstanceRange
}

func parseRangeFromString(s string) (ir pt.InstanceRange, err error) {
	// A default value of ":" yields from:0 to:MaxUint32. Specifying either side of the range
	// will only set that side, leaving the other default. i.e. ":200" will show the first 200 tasks, and
	// "50:" will show the tasks from 50 till the end
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return ir, fmt.Errorf("expected FROM:TO got '%s'", s)
	}
	from := uint32(0)
	to := uint32(math.MaxUint32)
	if parts[0] != "" {
		parsedFrom, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			return ir, err
		}
		from = uint32(parsedFrom)
	}
	ir.From = from

	if parts[1] != "" {
		parsedTo, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return ir, err
		}
		to = uint32(parsedTo)
	}
	ir.To = to
	return
}

// Set TaskRangeValue, implements kingpin.Value
func (v *TaskRangeValue) Set(value string) error {
	ir, err := parseRangeFromString(value)
	if err != nil {
		return err
	}
	(*pt.InstanceRange)(v).From = ir.From
	(*pt.InstanceRange)(v).To = ir.To
	return nil
}

// String TaskRangeValue, implements kingpin.Value
func (v *TaskRangeValue) String() string {
	return fmt.Sprintf("%d:%d", v.From, v.To)
}

// Set TaskRangeListValue, implements kingpin.Value
func (v *TaskRangeListValue) Set(value string) error {
	ir, err := parseRangeFromString(value)
	if err != nil {
		return err
	}
	accum := append(v.s, &ir)
	v.s = accum
	return nil
}

// String TaskRangeListValue, implements kingpin.Value
func (v *TaskRangeListValue) String() string {
	// Just stub this out so we implement kingpin.Value interface
	return ""
}

// IsCumulative TaskRangeListValue, implements kingpin.Value and allows for cumulative flags
func (v *TaskRangeListValue) IsCumulative() bool {
	return true
}

func taskRangeFlag(s kingpin.Settings) (target *pt.InstanceRange) {
	target = &pt.InstanceRange{}
	s.SetValue((*TaskRangeValue)(target))
	return
}

func taskRangeListFlag(s kingpin.Settings) (target *[]*pt.InstanceRange) {
	x := TaskRangeListValue{[]*pt.InstanceRange{}}
	target = &x.s
	s.SetValue(&x)
	return
}

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))
	var err error

	client, err := newClient(**frameworkURL, *timeout)
	if err != nil {
		app.FatalIfError(err, "")
	}
	defer client.Cleanup()

	switch cmd {
	case jobCreate.FullCommand():
		err = client.jobCreateAction(*jobCreateName, *jobCreateConfig)
	case jobDelete.FullCommand():
		err = client.jobDeleteAction(*jobDeleteName)
	case jobGet.FullCommand():
		err = client.jobGetAction(*jobGetName)
	case taskGet.FullCommand():
		err = client.taskGetAction(*taskGetJobName, *taskGetInstanceID)
	case taskList.FullCommand():
		err = client.taskListAction(*taskListJobName, taskListInstanceRange)
	case taskStart.FullCommand():
		err = client.taskStartAction(*taskStartJobName, *taskStartInstanceRanges)
	case taskStop.FullCommand():
		err = client.taskStopAction(*taskStopJobName, *taskStopInstanceRanges)
	case taskRestart.FullCommand():
		err = client.taskRestartAction(*taskRestartJobName, *taskRestartInstanceRanges)
	default:
		app.Fatalf("Unknown command %s", cmd)
	}
	app.FatalIfError(err, "")
}
