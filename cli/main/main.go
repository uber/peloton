package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	pt "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

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

	// TODO: deprecate jobMgrURL/resMgrURL/hostMgrURL once we fix pcluster container network
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
		Short('v').
		Default("http://localhost:5290").
		Envar("RESMGR_URL").
		URL()

	hostMgrURL = app.Flag(
		"hostmgr",
		"name of the host manager address to use (http/tchannel) (set $HOSTMGR_URL to override)").
		Short('u').
		Default("http://localhost:5291").
		Envar("HOSTMGR_URL").
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
	jobCreateConfig     = jobCreate.Arg("config", "YAML job configuration").Required().ExistingFile()
	jobCreateSecretPath = jobCreate.Flag("secret-path", "secret mount path").Default("").String()
	jobCreateSecret     = jobCreate.Flag("secret-data", "secret data string").Default("").String()

	jobDelete     = job.Command("delete", "delete a job")
	jobDeleteName = jobDelete.Arg("job", "job identifier").Required().String()

	jobStop         = job.Command("stop", "stop job(s) by job identifier, owning team or labels")
	jobStopName     = jobStop.Arg("job", "job identifier").Default("").String()
	jobStopProgress = jobStop.Flag("progress",
		"show progress of the job stopping").Default(
		"false").Bool()
	jobStopOwner  = jobStop.Flag("owner", "job owner").Default("").String()
	jobStopLabels = jobStop.Flag("labels", "job labels").Default("").Short('l').String()
	jobStopForce  = jobStop.Flag("force", "force stop").Default("false").Short('f').Bool()

	jobGet     = job.Command("get", "get a job")
	jobGetName = jobGet.Arg("job", "job identifier").Required().String()

	jobRefresh     = job.Command("refresh", "load runtime state of job and re-refresh corresponding action (debug only)")
	jobRefreshName = jobRefresh.Arg("job", "job identifier").Required().String()

	jobStatus     = job.Command("status", "get job status")
	jobStatusName = jobStatus.Arg("job", "job identifier").Required().String()

	// peloton -z zookeeper-peloton-devel01 job query --labels="x=y,a=b" --respool=xx --keywords=k1,k2 --states=running,killed --limit=1
	jobQuery            = job.Command("query", "query jobs by mesos label / respool")
	jobQueryLabels      = jobQuery.Flag("labels", "labels").Default("").Short('l').String()
	jobQueryRespoolPath = jobQuery.Flag("respool", "respool path").Default("").Short('r').String()
	jobQueryKeywords    = jobQuery.Flag("keywords", "keywords").Default("").Short('k').String()
	jobQueryStates      = jobQuery.Flag("states", "job states").Default("").Short('s').String()
	jobQueryOwner       = jobQuery.Flag("owner", "job owner").Default("").String()
	jobQueryName        = jobQuery.Flag("name", "job name").Default("").String()
	// We can search by time range for completed time as well as created time.
	// We support protobuf timestamps in backend to define time range
	// To keep CLI simple, lets accept this time range for creation time in last n days
	jobQueryTimeRange = jobQuery.Flag("timerange", "query jobs created within last d days").Short('d').Default("0").Uint32()
	jobQueryLimit     = jobQuery.Flag("limit", "maximum number of jobs to return").Default("100").Short('n').Uint32()
	jobQueryMaxLimit  = jobQuery.Flag("total", "total number of jobs to query").Default("100").Short('q').Uint32()
	jobQueryOffset    = jobQuery.Flag("offset", "offset").Default("0").Short('o').Uint32()
	jobQuerySortBy    = jobQuery.Flag("sort", "sort by property").Default("creation_time").Short('p').String()
	jobQuerySortOrder = jobQuery.Flag("sortorder", "sort order (ASC or DESC)").Default("DESC").Short('a').String()

	jobUpdate           = job.Command("update", "update a job")
	jobUpdateID         = jobUpdate.Arg("job", "job identifier").Required().String()
	jobUpdateConfig     = jobUpdate.Arg("config", "YAML job configuration").Required().ExistingFile()
	jobUpdateSecretPath = jobUpdate.Flag("secret-path", "secret mount path").Default("").String()
	jobUpdateSecret     = jobUpdate.Flag("secret-data", "secret data string").Default("").String()

	jobRestart                = job.Command("rolling-restart", "restart instances in a job using rolling-restart")
	jobRestartName            = jobRestart.Arg("job", "job identifier").Required().String()
	jobRestartBatchSize       = jobRestart.Arg("batch-size", "batch size for the restart").Required().Uint32()
	jobRestartResourceVersion = jobRestart.Flag("resourceVersion", "resource version of the job for concurrency control").Default("0").Uint64()
	jobRestartInstanceRanges  = taskRangeListFlag(jobRestart.Flag("range", "restart range of instances (specify multiple times) (from:to syntax, default ALL)").Default(":").Short('r'))

	jobStart                = job.Command("rolling-start", "start instances in a job using rolling-start")
	jobStartName            = jobStart.Arg("job", "job identifier").Required().String()
	jobStartBatchSize       = jobStart.Arg("batch-size", "batch size for the start").Required().Uint32()
	jobStartResourceVersion = jobStart.Flag("resourceVersion", "resource version of the job for concurrency control").Default("0").Uint64()
	jobStartInstanceRanges  = taskRangeListFlag(jobStart.Flag("range", "start range of instances (specify multiple times) (from:to syntax, default ALL)").Default(":").Short('r'))

	jobStopV1Beta                = job.Command("rolling-stop", "stop instances in a job using rolling-stop")
	jobStopV1BetaName            = jobStopV1Beta.Arg("job", "job identifier").Required().String()
	jobStopV1BetaBatchSize       = jobStopV1Beta.Arg("batch-size", "batch size for the stop").Required().Uint32()
	jobStopV1BetaResourceVersion = jobStopV1Beta.Flag("resourceVersion", "resource version of the job for concurrency control").Default("0").Uint64()
	jobStopV1BetaInstanceRanges  = taskRangeListFlag(jobStopV1Beta.Flag("range", "stop range of instances (specify multiple times) (from:to syntax, default ALL)").Default(":").Short('r'))

	jobGetCache     = job.Command("cache", "get a job cache")
	jobGetCacheName = jobGetCache.Arg("job", "job identifier").Required().String()

	// Top level pod command
	pod = app.Command("pod", "CLI reflects pod(s) actions, such as get pod details, create/restart/update a pod...")

	podGetEvents           = pod.Command("events", "get pod events in reverse chronological order.")
	podGetEventsJobName    = podGetEvents.Arg("job", "job identifier").Required().String()
	podGetEventsInstanceID = podGetEvents.Arg("instance", "job instance id").Required().Uint32()
	podGetEventsRunID      = podGetEvents.Flag("run", "get pod events for this runID only").Short('r').String()
	podGetEventsLimit      = podGetEvents.Flag("limit", "number of events to return, default value 100").Short('l').Uint64()

	podGetCache        = pod.Command("cache", "get pod status from cache")
	podGetCachePodName = podGetCache.Arg("name", "pod name").Required().String()

	podGetEventsV1Alpha        = pod.Command("events-v1alpha", "get pod events")
	podGetEventsV1AlphaPodName = podGetEventsV1Alpha.Arg("name", "pod name").Required().String()
	podGetEventsV1AlphaPodID   = podGetEventsV1Alpha.Flag("id", "pod identifier").Short('p').String()

	podRefresh        = pod.Command("refresh", "load pod status and re-refresh corresponding action (debug only)")
	podRefreshPodName = podRefresh.Arg("name", "pod name").Required().String()

	podStart        = pod.Command("start", "start a pod")
	podStartPodName = podStart.Arg("name", "pod name").Required().String()

	// Top level task command
	task = app.Command("task", "manage tasks")

	taskGet           = task.Command("get", "show task status")
	taskGetJobName    = taskGet.Arg("job", "job identifier").Required().String()
	taskGetInstanceID = taskGet.Arg("instance", "job instance id").Required().Uint32()

	taskGetCache           = task.Command("cache", "show task status")
	taskGetCacheName       = taskGetCache.Arg("job", "job identifier").Required().String()
	taskGetCacheInstanceID = taskGetCache.Arg("instance", "job instance id").Required().Uint32()

	taskGetEvents           = task.Command("events", "show task events")
	taskGetEventsJobName    = taskGetEvents.Arg("job", "job identifier").Required().String()
	taskGetEventsInstanceID = taskGetEvents.Arg("instance", "job instance id").Required().Uint32()

	taskLogsGet           = task.Command("logs", "show task logs")
	taskLogsGetFileName   = taskLogsGet.Flag("filename", "log filename to browse").Default("stdout").Short('f').String()
	taskLogsGetJobName    = taskLogsGet.Arg("job", "job identifier").Required().String()
	taskLogsGetInstanceID = taskLogsGet.Arg("instance", "job instance id").Required().Uint32()
	taskLogsGetTaskID     = taskLogsGet.Arg("taskId", "task identifier").Default("").String()

	taskList              = task.Command("list", "show tasks of a job")
	taskListJobName       = taskList.Arg("job", "job identifier").Required().String()
	taskListInstanceRange = taskRangeFlag(taskList.Flag("range", "show range of instances (from:to syntax)").Default(":").Short('r'))

	taskQuery          = task.Command("query", "query tasks by state(s)")
	taskQueryJobName   = taskQuery.Arg("job", "job identifier").Required().String()
	taskQueryStates    = taskQuery.Flag("states", "task states").Default("").Short('s').String()
	taskQueryTaskNames = taskQuery.Flag("names", "task names").Default("").String()
	taskQueryTaskHosts = taskQuery.Flag("hosts", "task hosts").Default("").String()
	taskQueryLimit     = taskQuery.Flag("limit", "limit").Default("100").Short('n').Uint32()
	taskQueryOffset    = taskQuery.Flag("offset", "offset").Default("0").Short('o').Uint32()
	taskQuerySortBy    = taskQuery.Flag("sort", "sort by property (creation_time, host, instance_id, message, name, reason, state)").Short('p').String()
	taskQuerySortOrder = taskQuery.Flag("sortorder", "sort order (ASC or DESC)").Short('a').Default("ASC").Enum("ASC", "DESC")

	taskRefresh              = task.Command("refresh", "load runtime state of tasks and re-refresh corresponding action (debug only)")
	taskRefreshJobName       = taskRefresh.Arg("job", "job identifier").Required().String()
	taskRefreshInstanceRange = taskRangeFlag(taskRefresh.Flag("range", "range of instances (from:to syntax)").Default(":").Short('r'))

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

	// Top level resource manager state command
	resMgr      = app.Command("resmgr", "fetch resource manager state")
	resMgrTasks = resMgr.Command("tasks", "fetch resource manager task state")

	resMgrActiveTasks             = resMgrTasks.Command("active", "fetch active tasks in resource manager")
	resMgrActiveTasksGetJobName   = resMgrActiveTasks.Flag("job", "job identifier").Default("").String()
	resMgrActiveTasksGetRespoolID = resMgrActiveTasks.Flag("respool", "resource pool identifier").Default("").String()
	resMgrActiveTasksGetStates    = resMgrActiveTasks.Flag("states", "task states").Default("").String()

	resMgrPendingTasks = resMgrTasks.Command("pending",
		"fetch pending tasks (ordered) grouped by gang, in resource manager"+
			" as json/yaml")
	resMgrPendingTasksGetRespoolID = resMgrPendingTasks.Arg("respool",
		"resource pool identifier").Required().String()
	resMgrPendingTasksGetLimit = resMgrPendingTasks.Flag("limit",
		"maximum number of gangs to return").Default("100").Uint32()

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

	// Top level host manager command
	host            = app.Command("host", "manage hosts")
	hostMaintenance = host.Command("maintenance", "host maintenance")

	hostMaintenanceStart          = hostMaintenance.Command("start", "start host maintenance on a list of hosts")
	hostMaintenanceStartHostnames = hostMaintenanceStart.Arg("hostnames", "comma separated hostnames").Required().String()

	hostMaintenanceComplete          = hostMaintenance.Command("complete", "complete host maintenance on a list of hosts")
	hostMaintenanceCompleteHostnames = hostMaintenanceComplete.Arg("hostnames", "comma separated hostnames").Required().String()

	hostQuery       = host.Command("query", "query hosts by state(s)")
	hostQueryStates = hostQuery.Flag("states", "host state(s) to filter").Default("").Short('s').String()

	// Top level volume command
	volume = app.Command("volume", "manage persistent volume")

	volumeList        = volume.Command("list", "list volumes for a job")
	volumeListJobName = volumeList.Arg("job", "job identifier").Required().String()

	volumeDelete         = volume.Command("delete", "delete a volume")
	volumeDeleteVolumeID = volumeDelete.Arg("volume", "volume identifier").Required().String()

	// Top level job update command
	update = app.Command("update", "manage job updates")

	// command to create a new job update
	updateCreate       = update.Command("create", "create a new job update")
	updateJobID        = updateCreate.Arg("job", "job identifier").Required().String()
	updateCreateConfig = updateCreate.Arg("config", "YAML job configuration").Required().ExistingFile()
	updateBatchSize    = updateCreate.Arg("batch-size", "batch size for the update").Required().Uint32()
	updateResPoolPath  = updateCreate.Arg("respool", "complete path of the "+
		"resource pool starting from the root").Required().String()
	updateConfigVersion = updateCreate.Flag("configuration-version",
		"current configuration version").Default("0").Short('c').Uint64()
	updateOverride = updateCreate.Flag("override",
		"override the existing update").Default("false").Short('o').Bool()
	updateMaxInstanceAttempts = updateCreate.Flag("maxattempts",
		"maximum retry attempts to bring up the instance after updating before marking it failed").Default("0").Uint32()
	updateMaxFailureInstances = updateCreate.Flag("maxfailureInstances",
		"maximum number of instance failures tolerable before failing the update").Default("0").Uint32()
	updateRollbackOnFailure = updateCreate.Flag("rollbackOnFailure",
		"rollback an update if it fails").Default("false").Bool()

	// command to fetch the status of a job update
	updateGet   = update.Command("get", "get status of a job update")
	updateGetID = updateGet.Arg("update-id", "update identifier").Required().String()

	// command to fetch the status of job updates for a given job
	updateList      = update.Command("list", "list status of all updates for a given job")
	updateListJobID = updateList.Arg("job", "job identifier").Required().String()

	// command to fetch the update information in the cache
	updateCache   = update.Command("cache", "get update information in  the cache")
	updateCacheID = updateCache.Arg("update-id", "update identifier").Required().String()

	// command to abort an update
	updateAbort   = update.Command("abort", "abort a job update")
	updateAbortID = updateAbort.Arg("update-id", "update identifier").Required().String()

	// command to pause an update
	updatePause   = update.Command("pause", "pause a job update")
	updatePauseID = updatePause.Arg("update-id", "update identifier").Required().String()

	// command to resume an update
	updateResume   = update.Command("resume", "resume a job update")
	updateResumeID = updateResume.Arg("update-id", "update identifier").Required().String()

	// top level command for offers
	offers = app.Command("offers", "get outstanding offers")

	offersList = offers.Command("list", "list all the outstanding offers")
)

// TaskRangeValue allows us to define a new target type for kingpin to allow specifying ranges of tasks with from:to syntax as a TaskRangeFlag
type TaskRangeValue pt.InstanceRange

// TaskRangeListValue allows for collecting kingpin Values with a cumulative flag parser for instance ranges
type TaskRangeListValue struct {
	s []*pt.InstanceRange
}

func parseRangeFromString(s string) (ir pt.InstanceRange, err error) {
	// A default value of ":" yields from:0 to:MaxInt32. Specifying either side of the range
	// will only set that side, leaving the other default. i.e. ":200" will show the first 200 tasks, and
	// "50:" will show the tasks from 50 till the end
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return ir, fmt.Errorf("expected FROM:TO got '%s'", s)
	}
	from := uint32(0)
	to := uint32(math.MaxInt32)
	if parts[0] != "" {
		parsedFrom, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			return ir, err
		}
		if parsedFrom < 0 {
			return ir, fmt.Errorf("unexpected negative FROM %d", parsedFrom)
		}
		from = uint32(parsedFrom)
	}
	ir.From = from

	if parts[1] != "" {
		parsedTo, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			return ir, err
		}
		if parsedTo < 0 {
			return ir, fmt.Errorf("unexpected negative TO %d", parsedTo)
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
		discovery, err = leader.NewStaticServiceDiscovery(*jobMgrURL, *resMgrURL, *hostMgrURL)
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
		err = client.JobCreateAction(*jobCreateID, *jobCreateResPoolPath,
			*jobCreateConfig, *jobCreateSecretPath, []byte(*jobCreateSecret))
	case jobDelete.FullCommand():
		err = client.JobDeleteAction(*jobDeleteName)
	case jobStop.FullCommand():
		err = client.JobStopAction(
			*jobStopName,
			*jobStopProgress,
			*jobStopOwner,
			*jobStopLabels,
			*jobStopForce,
		)
	case jobGet.FullCommand():
		err = client.JobGetAction(*jobGetName)
	case jobRefresh.FullCommand():
		err = client.JobRefreshAction(*jobRefreshName)
	case jobStatus.FullCommand():
		err = client.JobStatusAction(*jobStatusName)
	case jobQuery.FullCommand():
		err = client.JobQueryAction(*jobQueryLabels, *jobQueryRespoolPath, *jobQueryKeywords, *jobQueryStates, *jobQueryOwner, *jobQueryName, *jobQueryTimeRange, *jobQueryLimit, *jobQueryMaxLimit, *jobQueryOffset, *jobQuerySortBy, *jobQuerySortOrder)
	case jobUpdate.FullCommand():
		err = client.JobUpdateAction(*jobUpdateID, *jobUpdateConfig,
			*jobUpdateSecretPath, []byte(*jobUpdateSecret))
	case jobRestart.FullCommand():
		err = client.JobRestartAction(*jobRestartName, *jobRestartResourceVersion, *jobRestartInstanceRanges, *jobRestartBatchSize)
	case jobStart.FullCommand():
		err = client.JobStartAction(*jobStartName, *jobStartResourceVersion, *jobStartInstanceRanges, *jobStartBatchSize)
	case jobStopV1Beta.FullCommand():
		err = client.JobStopV1BetaAction(*jobStopV1BetaName, *jobStopV1BetaResourceVersion, *jobStopV1BetaInstanceRanges, *jobStopV1BetaBatchSize)
	case jobGetCache.FullCommand():
		err = client.JobGetCacheAction(*jobGetCacheName)
	case taskGet.FullCommand():
		err = client.TaskGetAction(*taskGetJobName, *taskGetInstanceID)
	case taskGetCache.FullCommand():
		err = client.TaskGetCacheAction(*taskGetCacheName, *taskGetCacheInstanceID)
	case taskGetEvents.FullCommand():
		err = client.TaskGetEventsAction(*taskGetEventsJobName, *taskGetEventsInstanceID)
	case taskLogsGet.FullCommand():
		err = client.TaskLogsGetAction(*taskLogsGetFileName, *taskLogsGetJobName, *taskLogsGetInstanceID, *taskLogsGetTaskID)
	case taskList.FullCommand():
		err = client.TaskListAction(*taskListJobName, taskListInstanceRange)
	case taskQuery.FullCommand():
		err = client.TaskQueryAction(*taskQueryJobName, *taskQueryStates, *taskQueryTaskNames, *taskQueryTaskHosts, *taskQueryLimit, *taskQueryOffset, *taskQuerySortBy, *taskQuerySortOrder)
	case taskRefresh.FullCommand():
		err = client.TaskRefreshAction(*taskRefreshJobName, taskRefreshInstanceRange)
	case taskStart.FullCommand():
		err = client.TaskStartAction(*taskStartJobName, *taskStartInstanceRanges)
	case taskStop.FullCommand():
		err = client.TaskStopAction(*taskStopJobName,
			*taskStopInstanceRanges)
	case taskRestart.FullCommand():
		err = client.TaskRestartAction(*taskRestartJobName, *taskRestartInstanceRanges)
	case hostMaintenanceStart.FullCommand():
		err = client.HostMaintenanceStartAction(*hostMaintenanceStartHostnames)
	case hostMaintenanceComplete.FullCommand():
		err = client.HostMaintenanceCompleteAction(*hostMaintenanceCompleteHostnames)
	case hostQuery.FullCommand():
		err = client.HostQueryAction(*hostQueryStates)
	case resMgrActiveTasks.FullCommand():
		err = client.ResMgrGetActiveTasks(*resMgrActiveTasksGetJobName, *resMgrActiveTasksGetRespoolID, *resMgrActiveTasksGetStates)
	case resMgrPendingTasks.FullCommand():
		err = client.ResMgrGetPendingTasks(*resMgrPendingTasksGetRespoolID,
			uint32(*resMgrPendingTasksGetLimit))
	case resPoolCreate.FullCommand():
		err = client.ResPoolCreateAction(*resPoolCreatePath, *resPoolCreateConfig)
	case respoolUpdate.FullCommand():
		err = client.ResPoolUpdateAction(*respoolUpdatePath, *respoolUpdateConfig)
	case resPoolDump.FullCommand():
		err = client.ResPoolDumpAction(*resPoolDumpFormat)
	case resPoolDelete.FullCommand():
		err = client.ResPoolDeleteAction(*resPoolDeletePath)
	case volumeList.FullCommand():
		err = client.VolumeListAction(*volumeListJobName)
	case volumeDelete.FullCommand():
		err = client.VolumeDeleteAction(*volumeDeleteVolumeID)
	case updateCreate.FullCommand():
		err = client.UpdateCreateAction(
			*updateJobID,
			*updateCreateConfig,
			*updateBatchSize,
			*updateResPoolPath,
			*updateConfigVersion,
			*updateOverride,
			*updateMaxInstanceAttempts,
			*updateMaxFailureInstances,
			*updateRollbackOnFailure,
		)
	case updateGet.FullCommand():
		err = client.UpdateGetAction(*updateGetID)
	case updateList.FullCommand():
		err = client.UpdateListAction(*updateListJobID)
	case updateCache.FullCommand():
		err = client.UpdateGetCacheAction(*updateCacheID)
	case updateAbort.FullCommand():
		err = client.UpdateAbortAction(*updateAbortID)
	case updatePause.FullCommand():
		err = client.UpdatePauseAction(*updatePauseID)
	case updateResume.FullCommand():
		err = client.UpdateResumeAction(*updateResumeID)
	case offersList.FullCommand():
		err = client.OffersGetAction()
	case podGetEvents.FullCommand():
		err = client.PodGetEventsAction(*podGetEventsJobName, *podGetEventsInstanceID, *podGetEventsRunID, *podGetEventsLimit)
	case podGetCache.FullCommand():
		err = client.PodGetCache(*podGetCachePodName)
	case podGetEvents.FullCommand():
		err = client.PodGetEventsAction(*podGetEventsJobName, *podGetEventsInstanceID, *podGetEventsRunID, *podGetEventsLimit)
	case podGetEventsV1Alpha.FullCommand():
		err = client.PodGetEventsV1AlphaAction(*podGetEventsV1AlphaPodName, *podGetEventsV1AlphaPodID)
	case podRefresh.FullCommand():
		err = client.PodRefresh(*podRefreshPodName)
	case podStart.FullCommand():
		err = client.PodStart(*podStartPodName)

	default:
		app.Fatalf("Unknown command %s", cmd)
	}
	app.FatalIfError(err, "")
}
