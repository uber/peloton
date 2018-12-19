package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"code.uber.internal/infra/peloton/util"

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
		"name of the jobmgr address to use (grpc) (set $JOBMGR_URL to override)").
		Short('m').
		Default("localhost:5392").
		Envar("JOBMGR_URL").
		URL()

	resMgrURL = app.Flag(
		"resmgr",
		"name of the resource manager address to use (grpc) (set $RESMGR_URL to override)").
		Short('v').
		Default("localhost:5394").
		Envar("RESMGR_URL").
		URL()

	hostMgrURL = app.Flag(
		"hostmgr",
		"name of the host manager address to use (grpc) (set $HOSTMGR_URL to override)").
		Short('u').
		Default("localhost:5391").
		Envar("HOSTMGR_URL").
		URL()

	clusterName = app.Flag(
		"clusterName",
		"name of the cluster you want to connect to."+
			"e.g pit1-preprod01").
		Short('e').
		Envar("CLUSTER_NAME").
		String()

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

	jobGetActiveJobs = job.Command("active-list", "get a list of active jobs")

	// Top level job command for stateless jobs
	stateless = job.Command("stateless", "manage stateless jobs")

	statelessGetCache     = stateless.Command("cache", "get a job cache")
	statelessGetCacheName = statelessGetCache.Arg("job", "job identifier").Required().String()

	statelessRefresh     = stateless.Command("refresh", "refresh a job")
	statelessRefreshName = statelessRefresh.Arg("job", "job identifier").Required().String()

	workflow                   = stateless.Command("workflow", "manage workflow for stateless job")
	workflowPause              = workflow.Command("pause", "pause a workflow")
	workflowPauseName          = workflowPause.Arg("job", "job identifier").Required().String()
	workflowPauseEntityVersion = workflowPause.Arg("entityVersion",
		"entity version for concurrency control").Required().String()
	workflowPauseOpaqueData = workflowPause.Flag("opaque-data",
		"opaque data provided by the user").Default("").String()

	workflowResume              = workflow.Command("resume", "resume a workflow")
	workflowResumeName          = workflowResume.Arg("job", "job identifier").Required().String()
	workflowResumeEntityVersion = workflowResume.Arg("entityVersion",
		"entity version for concurrency control").Required().String()
	workflowResumeOpaqueData = workflowResume.Flag("opaque-data",
		"opaque data provided by the user").Default("").String()

	workflowAbort              = workflow.Command("abort", "abort a workflow")
	workflowAbortName          = workflowAbort.Arg("job", "job identifier").Required().String()
	workflowAbortEntityVersion = workflowAbort.Arg("entityVersion",
		"entity version for concurrency control").Required().String()
	workflowAbortOpaqueData = workflowAbort.Flag("opaque-data",
		"opaque data provided by the user").Default("").String()

	statelessQuery            = stateless.Command("query", "query stateless jobs by mesos label / respool")
	statelessQueryLabels      = statelessQuery.Flag("labels", "labels").Default("").Short('l').String()
	statelessQueryRespoolPath = statelessQuery.Flag("respool", "respool path").Default("").Short('r').String()
	statelessQueryKeywords    = statelessQuery.Flag("keywords", "keywords").Default("").Short('k').String()
	statelessQueryStates      = statelessQuery.Flag("states", "job states").Default("").Short('s').String()
	statelessQueryOwner       = statelessQuery.Flag("owner", "job owner").Default("").String()
	statelessQueryName        = statelessQuery.Flag("name", "job name").Default("").String()
	// We can search by time range for completed time as well as created time.
	// We support protobuf timestamps in backend to define time range
	// To keep CLI simple, lets accept this time range for creation time in last n days
	statelessQueryTimeRange = statelessQuery.Flag("timerange", "query jobs created within last d days").Short('d').Default("0").Uint32()
	statelessQueryLimit     = statelessQuery.Flag("limit", "maximum number of jobs to return").Default("100").Short('n').Uint32()
	statelessQueryMaxLimit  = statelessQuery.Flag("total", "total number of jobs to query").Default("100").Short('q').Uint32()
	statelessQueryOffset    = statelessQuery.Flag("offset", "offset").Default("0").Short('o').Uint32()
	statelessQuerySortBy    = statelessQuery.Flag("sort", "sort by property").Default("creation_time").Short('p').String()
	statelessQuerySortOrder = statelessQuery.Flag("sortorder", "sort order (ASC or DESC)").Default("DESC").Short('a').String()

	statelessReplace            = stateless.Command("replace", "update by replacing job config")
	statelessReplaceJobID       = statelessReplace.Arg("job", "job identifier").Required().String()
	statelessReplaceSpec        = statelessReplace.Arg("spec", "YAML job spec").Required().ExistingFile()
	statelessReplaceBatchSize   = statelessReplace.Arg("batch-size", "batch size for the update").Required().Uint32()
	statelessReplaceResPoolPath = statelessReplace.Arg("respool", "complete path of the "+
		"resource pool starting from the root").Required().String()
	statelessReplaceEntityVersion = statelessReplace.Arg("entityVersion",
		"entity version for concurrency control").Required().String()
	statelessReplaceOverride = statelessReplace.Flag("override",
		"override the existing update").Default("false").Short('o').Bool()
	statelessReplaceMaxInstanceRetries = statelessReplace.Flag(
		"maxInstanceRetries",
		"maximum instance retries to bring up the instance after updating before marking it failed."+
			"If the value is 0, the instance can be retried for infinite times.").Default("0").Uint32()
	statelessReplaceMaxTolerableInstanceFailures = statelessReplace.Flag(
		"maxTolerableInstanceFailures",
		"maximum number of instance failures tolerable before failing the update."+
			"If the value is 0, there is no limit for max failure instances and"+
			"the update is marked successful even if all of the instances fail.").Default("0").Uint32()
	statelessReplaceRollbackOnFailure = statelessReplace.Flag("rollbackOnFailure",
		"rollback an update if it fails").Default("false").Bool()
	statelessReplaceStartPaused = statelessReplace.Flag("start-paused",
		"start the update in a paused state").Default("false").Bool()
	statelessReplaceOpaqueData = statelessReplace.Flag("opaque-data",
		"opaque data provided by the user").Default("").String()

	statelessListJobs = stateless.Command("list", "list all jobs")

	statelessCreate            = stateless.Command("create", "create stateless job")
	statelessCreateResPoolPath = statelessCreate.Arg("respool", "complete path of the "+
		"resource pool starting from the root").Required().String()
	statelessCreateSpec       = statelessCreate.Arg("spec", "YAML job specification").Required().ExistingFile()
	statelessCreateID         = statelessCreate.Flag("jobID", "optional job identifier, must be UUID format").Short('i').String()
	statelessCreateSecretPath = statelessCreate.Flag("secret-path", "secret mount path").Default("").String()
	statelessCreateSecret     = statelessCreate.Flag("secret-data", "secret data string").Default("").String()

	statelessReplaceJobDiff = stateless.Command("replace-diff",
		"dry-run of replace to the the instances to be added/removed/updated/unchanged")
	statelessReplaceJobDiffJobID       = statelessReplaceJobDiff.Arg("job", "job identifier").Required().String()
	statelessReplaceJobDiffSpec        = statelessReplaceJobDiff.Arg("spec", "YAML job spec").Required().ExistingFile()
	statelessReplaceJobDiffResPoolPath = statelessReplaceJobDiff.Arg("respool", "complete path of the "+
		"resource pool starting from the root").Required().String()
	statelessReplaceJobDiffEntityVersion = statelessReplaceJobDiff.Arg("entityVersion",
		"entity version for concurrency control").Required().String()

	// Top level pod command
	pod = app.Command("pod", "CLI reflects pod(s) actions, such as get pod details, create/restart/update a pod...")

	podGetEvents           = pod.Command("events", "get pod events in reverse chronological order.")
	podGetEventsJobName    = podGetEvents.Arg("job", "job identifier").Required().String()
	podGetEventsInstanceID = podGetEvents.Arg("instance", "job instance id").Required().Uint32()
	podGetEventsRunID      = podGetEvents.Flag("run", "get pod events for this runID only").Short('r').String()
	podGetEventsLimit      = podGetEvents.Flag("limit", "limit to last n runs of the pod, default value 10").Short('l').Uint64()

	podGetCache        = pod.Command("cache", "get pod status from cache")
	podGetCachePodName = podGetCache.Arg("name", "pod name").Required().String()

	podGetEventsV1Alpha        = pod.Command("events-v1alpha", "get pod events")
	podGetEventsV1AlphaPodName = podGetEventsV1Alpha.Arg("name", "pod name").Required().String()
	podGetEventsV1AlphaPodID   = podGetEventsV1Alpha.Flag("id", "pod identifier").Short('p').String()

	podRefresh        = pod.Command("refresh", "load pod status and re-refresh corresponding action (debug only)")
	podRefreshPodName = podRefresh.Arg("name", "pod name").Required().String()

	podStart        = pod.Command("start", "start a pod")
	podStartPodName = podStart.Arg("name", "pod name").Required().String()

	podLogsGet         = pod.Command("logs", "show pod logs")
	podLogsGetFileName = podLogsGet.Flag("filename", "log filename to browse").Default("stdout").Short('f').String()
	podLogsGetPodName  = podLogsGet.Arg("name", "pod name").Required().String()
	podLogsGetPodID    = podLogsGet.Flag("id", "pod identifier").Short('p').String()

	podRestart     = pod.Command("restart", "restart a pod")
	podRestartName = podRestart.Arg("name", "pod name").Required().String()

	podStop        = pod.Command("stop", "stop a pod")
	podStopPodName = podStop.Arg("name", "pod name").Required().String()

	podGet           = pod.Command("get", "get pod info")
	podGetPodName    = podGet.Arg("name", "pod name").Required().String()
	podGetStatusOnly = podGet.Flag("statusonly", "get pod status only(not spec)").Default("false").Bool()

	podDeleteEvents        = pod.Command("delete-events", "delete pod events")
	podDeleteEventsPodName = podDeleteEvents.Arg("name", "pod name").Required().String()
	podDeleteEventsPodID   = podDeleteEvents.Arg("id", "pod identifier").Required().String()

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
	updateStartInPausedState = updateCreate.Flag("start-paused",
		"start the update in a paused state").Default("false").Bool()
	updateCreateOpaqueData = updateCreate.Flag("opaque-data",
		"opaque data provided by the user").Default("").String()

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
	updateAbort           = update.Command("abort", "abort a job update")
	updateAbortID         = updateAbort.Arg("update-id", "update identifier").Required().String()
	updateAbortOpaqueData = updateAbort.Flag("opaque-data",
		"opaque data provided by the user").Default("").String()

	// command to pause an update
	updatePause           = update.Command("pause", "pause a job update")
	updatePauseID         = updatePause.Arg("update-id", "update identifier").Required().String()
	updatePauseOpaqueData = updatePause.Flag("opaque-data",
		"opaque data provided by the user").Default("").String()

	// command to resume an update
	updateResume           = update.Command("resume", "resume a job update")
	updateResumeID         = updateResume.Arg("update-id", "update identifier").Required().String()
	updateResumeOpaqueData = updateResume.Flag("opaque-data",
		"opaque data provided by the user").Default("").String()

	// Top level hostmgr command
	hostmgr = app.Command("hostmgr", "top level command for hostmgr")

	// command for list offers
	offers = hostmgr.Command("offers", "list all outstanding offers")

	// command for listing hosts
	getHosts          = hostmgr.Command("hosts", "list all hosts matching the query")
	getHostsCPU       = getHosts.Flag("cpu", "compare cpu cores available at the host, ignore if not provided").Short('c').Default("0").Float64()
	getHostsGPU       = getHosts.Flag("gpu", "compare gpu cores available at the host, ignore if not provided").Short('g').Default("0").Float64()
	getHostsCmpLess   = getHosts.Flag("less", "list hosts with resources less than cpu and/or gpu cores specified (default to greater than and equal to if not specified)").Short('l').Default("false").Bool()
	getHostsHostnames = getHosts.Flag("hosts", "filter the hosts based on the comma separated hostnames provided").String()

	// command for list status update events present in the event stream
	eventStream = hostmgr.Command("events", "list all the task status update events present in event stream")
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

	if len(*clusterName) > 0 {
		var zkInfo string
		zkInfo, err = pc.GetZkInfoFromClusterName(*clusterName)
		if err != nil {
			app.FatalIfError(err, "Fail to get zk info for this cluster")
		}
		zkInfoSlice := strings.Split(zkInfo, ",")
		// if user provides both cluster name and zk info, check whether the information matches
		if len(*zkServers) > 0 {
			for _, a := range *zkServers {
				if !util.Contains(zkInfoSlice, a) {
					app.Fatalf("zk info of cluster %s mismatch with provided zk server %s, please correct/remove the cluster name or zk server", *clusterName, a)
				}
			}
		}
		zkServers = &zkInfoSlice
	}
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
	case jobGetActiveJobs.FullCommand():
		err = client.JobGetActiveJobsAction()
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
			*updateStartInPausedState,
			*updateCreateOpaqueData,
		)
	case updateGet.FullCommand():
		err = client.UpdateGetAction(*updateGetID)
	case updateList.FullCommand():
		err = client.UpdateListAction(*updateListJobID)
	case updateCache.FullCommand():
		err = client.UpdateGetCacheAction(*updateCacheID)
	case updateAbort.FullCommand():
		err = client.UpdateAbortAction(*updateAbortID, *updateAbortOpaqueData)
	case updatePause.FullCommand():
		err = client.UpdatePauseAction(*updatePauseID, *updatePauseOpaqueData)
	case updateResume.FullCommand():
		err = client.UpdateResumeAction(*updateResumeID, *updateResumeOpaqueData)
	case offers.FullCommand():
		err = client.OffersGetAction()
	case getHosts.FullCommand():
		err = client.HostsGetAction(*getHostsCPU, *getHostsGPU, *getHostsCmpLess, *getHostsHostnames)
	case podGetEvents.FullCommand():
		err = client.PodGetEventsAction(*podGetEventsJobName, *podGetEventsInstanceID, *podGetEventsRunID, *podGetEventsLimit)
	case podGetCache.FullCommand():
		err = client.PodGetCacheAction(*podGetCachePodName)
	case podGetEventsV1Alpha.FullCommand():
		err = client.PodGetEventsV1AlphaAction(*podGetEventsV1AlphaPodName, *podGetEventsV1AlphaPodID)
	case podRefresh.FullCommand():
		err = client.PodRefreshAction(*podRefreshPodName)
	case podStart.FullCommand():
		err = client.PodStartAction(*podStartPodName)
	case eventStream.FullCommand():
		err = client.EventStreamAction()
	case statelessListJobs.FullCommand():
		err = client.StatelessListJobsAction()
	case statelessGetCache.FullCommand():
		err = client.StatelessGetCacheAction(*statelessGetCacheName)
	case statelessRefresh.FullCommand():
		err = client.StatelessRefreshAction(*statelessRefreshName)
	case workflowPause.FullCommand():
		err = client.StatelessWorkflowPauseAction(
			*workflowPauseName,
			*workflowPauseEntityVersion,
			*workflowPauseOpaqueData,
		)
	case workflowResume.FullCommand():
		err = client.StatelessWorkflowResumeAction(
			*workflowResumeName,
			*workflowResumeEntityVersion,
			*workflowResumeOpaqueData,
		)
	case workflowAbort.FullCommand():
		err = client.StatelessWorkflowAbortAction(
			*workflowAbortName,
			*workflowAbortEntityVersion,
			*workflowAbortOpaqueData,
		)
	case statelessQuery.FullCommand():
		err = client.StatelessQueryAction(*statelessQueryLabels, *statelessQueryRespoolPath, *statelessQueryKeywords, *statelessQueryStates, *statelessQueryOwner, *statelessQueryName, *statelessQueryTimeRange, *statelessQueryLimit, *statelessQueryMaxLimit, *statelessQueryOffset, *statelessQuerySortBy, *statelessQuerySortOrder)
	case statelessReplace.FullCommand():
		err = client.StatelessReplaceJobAction(
			*statelessReplaceJobID,
			*statelessReplaceSpec,
			*statelessReplaceBatchSize,
			*statelessReplaceResPoolPath,
			*statelessReplaceEntityVersion,
			*statelessReplaceOverride,
			*statelessReplaceMaxInstanceRetries,
			*statelessReplaceMaxTolerableInstanceFailures,
			*statelessReplaceRollbackOnFailure,
			*statelessReplaceStartPaused,
			*statelessReplaceOpaqueData,
		)
	case statelessReplaceJobDiff.FullCommand():
		err = client.StatelessReplaceJobDiffAction(
			*statelessReplaceJobDiffJobID,
			*statelessReplaceJobDiffSpec,
			*statelessReplaceJobDiffEntityVersion,
			*statelessReplaceJobDiffResPoolPath,
		)
	case statelessCreate.FullCommand():
		err = client.StatelessCreateAction(
			*statelessCreateID,
			*statelessCreateResPoolPath,
			*statelessCreateSpec,
			*statelessCreateSecretPath,
			[]byte(*statelessCreateSecret),
		)
	case podLogsGet.FullCommand():
		err = client.PodLogsGetAction(*podLogsGetFileName, *podLogsGetPodName, *podLogsGetPodID)
	case podRestart.FullCommand():
		err = client.PodRestartAction(*podRestartName)
	case podStop.FullCommand():
		err = client.PodStopAction(*podStopPodName)
	case podGet.FullCommand():
		err = client.PodGetAction(*podGetPodName, *podGetStatusOnly)
	case podDeleteEvents.FullCommand():
		err = client.PodDeleteEvents(*podDeleteEventsPodName, *podDeleteEventsPodID)
	default:
		app.Fatalf("Unknown command %s", cmd)
	}
	app.FatalIfError(err, "")
}
