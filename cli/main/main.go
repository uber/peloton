package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	pt "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	pc "code.uber.internal/infra/peloton/cli"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/leader"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	// Version of the peloton cli. will be set by Makefile
	version string

	app = kingpin.New("peloton", "CLI for interacting with peloton")

	// Global CLI flags
	jsonFormat = app.Flag(
		"json",
		"print full json responses").
		Short('j').
		Default("false").
		Bool()

	// TODO: deprecate jobMgrURL/resMgrURL once we fix pcluster container network
	//       and make sure that local cli can access Uber Prodution hostname/ip
	jobMgrURL = app.Flag(
		"jobmgr",
		"name of the jobmgr address to use (http/tchannel) (set $JOBMGR_URL to override)").
		Short('m').
		Default("http://localhost:5292").
		Envar("JOBMGR_URL").
		URL()

	resMgrURL = app.Flag(
		"resmgr",
		"name of the resource manager address to use (http/tchannel) (set $RESMGR_URL to override)").
		Short('e').
		Default("http://localhost:5290").
		Envar("RESMGR_URL").
		URL()

	zkServers = app.Flag(
		"zkservers",
		"zookeeper servers used for peloton service discovery. "+
			"Specify multiple times for multiple servers"+
			"(set $ZK_SERVERS to override with '\n' as delimiter)").
		Short('z').
		Envar("ZK_SERVERS").
		Strings()

	zkRoot = app.Flag(
		"zkroot",
		"zookeeper root path for peloton service discovery(set $ZK_ROOT to override)").
		Default(common.DefaultLeaderElectionRoot).
		Envar("ZK_ROOT").
		String()

	timeout = app.Flag(
		"timeout",
		"default RPC timeout (set $TIMEOUT to override)").
		Default("20s").
		Short('t').
		Envar("TIMEOUT").
		Duration()

	// Top level job command
	job = app.Command("job", "manage jobs")

	jobCreate            = job.Command("create", "create a job")
	jobCreateID          = jobCreate.Flag("jobID", "optional job identifier, must be UUID format").Short('i').String()
	jobCreateResPoolPath = jobCreate.Arg("respool", "complete path of the "+
		"resource pool starting from the root").Required().String()
	jobCreateConfig = jobCreate.Arg("config", "YAML job configuration").Required().ExistingFile()

	jobDelete     = job.Command("cancel", "cancel a job").Alias("delete")
	jobDeleteName = jobDelete.Arg("job", "job identifier").Required().String()

	jobGet     = job.Command("get", "get a job")
	jobGetName = jobGet.Arg("job", "job identifier").Required().String()

	jobStatus     = job.Command("status", "get job status")
	jobStatusName = jobStatus.Arg("job", "job identifier").Required().String()

	// peloton -z zookeeper-peloton-devel01 job query --labels="x=y,a=b" --respool=xx --keywords=k1,k2 --states=running,killed --limit=1
	jobQuery            = job.Command("query", "query jobs by mesos label / respool")
	jobQueryLabels      = jobQuery.Flag("labels", "labels").Default("").Short('l').String()
	jobQueryRespoolPath = jobQuery.Flag("respool", "respool path").Default("").Short('r').String()
	jobQueryKeywords    = jobQuery.Flag("keywords", "keywords").Default("").Short('k').String()
	jobQueryStates      = jobQuery.Flag("states", "job states").Default("").Short('s').String()
	jobQueryLimit       = jobQuery.Flag("limit", "limit").Default("100").Short('n').Uint32()
	jobQueryOffset      = jobQuery.Flag("offset", "offset").Default("0").Short('o').Uint32()

	jobUpdate       = job.Command("update", "update a job")
	jobUpdateID     = jobUpdate.Arg("job", "job identifier").Required().String()
	jobUpdateConfig = jobUpdate.Arg("config", "YAML job configuration").Required().ExistingFile()

	// Top level task command
	task = app.Command("task", "manage tasks")

	taskGet           = task.Command("get", "show task status")
	taskGetJobName    = taskGet.Arg("job", "job identifier").Required().String()
	taskGetInstanceID = taskGet.Arg("instance", "job instance id").Required().Uint32()

	taskLogsGet           = task.Command("logs", "show task logs")
	taskLogsGetFileName   = taskLogsGet.Flag("filename", "log filename to browse").Default("stdout").Short('f').String()
	taskLogsGetJobName    = taskLogsGet.Arg("job", "job identifier").Required().String()
	taskLogsGetInstanceID = taskLogsGet.Arg("instance", "job instance id").Required().Uint32()

	taskList              = task.Command("list", "show tasks of a job")
	taskListJobName       = taskList.Arg("job", "job identifier").Required().String()
	taskListInstanceRange = taskRangeFlag(taskList.Flag("range", "show range of instances (from:to syntax)").Default(":").Short('r'))

	taskStart               = task.Command("start", "start a task")
	taskStartJobName        = taskStart.Arg("job", "job identifier").Required().String()
	taskStartInstanceRanges = taskRangeListFlag(taskStart.Flag("range", "start range of instances (specify multiple times) (from:to syntax, default ALL)").Default(":").Short('r'))

	taskStop        = task.Command("stop", "stop tasks in the job. If no instances specified, then stop all tasks")
	taskStopJobName = taskStop.Arg("job", "job identifier").Required().String()
	// TODO(mu): Add support for --instances=1,3,5 for better cli experience.
	taskStopInstanceRanges = taskRangeListFlag(taskStop.Flag("range", "stop range of instances (specify multiple times) (from:to syntax, default ALL)").Short('r'))

	taskRestart               = task.Command("restart", "restart a task")
	taskRestartJobName        = taskRestart.Arg("job", "job identifier").Required().String()
	taskRestartInstanceRanges = taskRangeListFlag(taskRestart.Flag("range", "restart range of instances (specify multiple times) (from:to syntax, default ALL)").Default(":").Short('r'))

	// Top level resource pool command
	resPool = app.Command("respool", "manage resource pools")

	resPoolCreate     = resPool.Command("create", "create a resource pool")
	resPoolCreatePath = resPoolCreate.Arg("respool", "complete path of the "+
		"resource pool starting from the root").Required().String()
	resPoolCreateConfig = resPoolCreate.Arg("config", "YAML Resource Pool configuration").Required().ExistingFile()

	respoolUpdate     = resPool.Command("update", "update an existing resource pool")
	respoolUpdatePath = respoolUpdate.Arg("respool", "complete path of the "+
		"resource pool starting from the root").Required().String()
	respoolUpdateConfig = respoolUpdate.Arg("config", "YAML Resource Pool configuration").Required().ExistingFile()

	resPoolDump = resPool.Command(
		"dump",
		"Dump all resource pool(s)",
	)
	resPoolDumpFormat = resPoolDump.Flag(
		"format",
		"Dump resource pool(s) in a format - default (yaml)",
	).Default("yaml").Enum("yaml", "yml", "json")

	resPoolDelete     = resPool.Command("delete", "delete a resource pool")
	resPoolDeletePath = resPoolDelete.Arg("respool", "complete path of the "+
		"resource pool starting from the root").Required().String()
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

	var discovery leader.Discovery
	if len(*zkServers) > 0 {
		discovery, err = leader.NewZkServiceDiscovery(*zkServers, *zkRoot)
	} else {
		discovery, err = leader.NewStaticServiceDiscovery(*jobMgrURL, *resMgrURL)
	}
	if err != nil {
		app.FatalIfError(err, "Fail to initialize service discovery")
	}

	client, err := pc.New(discovery, *timeout, *jsonFormat)
	if err != nil {
		app.FatalIfError(err, "Fail to initialize client")
	}
	defer client.Cleanup()

	switch cmd {
	case jobCreate.FullCommand():
		err = client.JobCreateAction(*jobCreateID, *jobCreateResPoolPath, *jobCreateConfig)
	case jobDelete.FullCommand():
		err = client.JobDeleteAction(*jobDeleteName)
	case jobGet.FullCommand():
		err = client.JobGetAction(*jobGetName)
	case jobStatus.FullCommand():
		err = client.JobStatusAction(*jobStatusName)
	case jobQuery.FullCommand():
		err = client.JobQueryAction(*jobQueryLabels, *jobQueryRespoolPath, *jobQueryKeywords, *jobQueryStates, *jobQueryLimit, *jobQueryOffset)
	case jobUpdate.FullCommand():
		err = client.JobUpdateAction(*jobUpdateID, *jobUpdateConfig)
	case taskGet.FullCommand():
		err = client.TaskGetAction(*taskGetJobName, *taskGetInstanceID)
	case taskLogsGet.FullCommand():
		err = client.TaskLogsGetAction(*taskLogsGetFileName, *taskLogsGetJobName, *taskLogsGetInstanceID)
	case taskList.FullCommand():
		err = client.TaskListAction(*taskListJobName, taskListInstanceRange)
	case taskStart.FullCommand():
		err = client.TaskStartAction(*taskStartJobName, *taskStartInstanceRanges)
	case taskStop.FullCommand():
		err = client.TaskStopAction(*taskStopJobName, *taskStopInstanceRanges)
	case taskRestart.FullCommand():
		err = client.TaskRestartAction(*taskRestartJobName, *taskRestartInstanceRanges)
	case resPoolCreate.FullCommand():
		err = client.ResPoolCreateAction(*resPoolCreatePath, *resPoolCreateConfig)
	case respoolUpdate.FullCommand():
		err = client.ResPoolUpdateAction(*respoolUpdatePath, *respoolUpdateConfig)
	case resPoolDump.FullCommand():
		err = client.ResPoolDumpAction(*resPoolDumpFormat)
	case resPoolDelete.FullCommand():
		err = client.ResPoolDeleteAction(*resPoolDeletePath)
	default:
		app.Fatalf("Unknown command %s", cmd)
	}
	app.FatalIfError(err, "")
}
