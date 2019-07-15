// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"math"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	hostmgrutil "github.com/uber/peloton/pkg/hostmgr/util"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
)

const (
	// PelotonJobIDLabelKey is the task label key for job ID
	PelotonJobIDLabelKey = "peloton.job_id"
	// PelotonInstanceIDLabelKey is the task label key for task instance ID
	PelotonInstanceIDLabelKey = "peloton.instance_id"
	// PelotonTaskIDLabelKey is the task label key for task ID
	PelotonTaskIDLabelKey = "peloton.task_id"

	// Set default task kill grace period to 30 seconds
	_defaultTaskKillGracePeriod = 30 * time.Second

	// Default custom executor id prefix
	_defaultCustomExecutorPrefix = "thermos-"

	// Default custom executor name
	_defaultCustomExecutorName = "AuroraExecutor"
)

var (
	_pelotonRole      = "peloton"
	_pelotonPrinciple = "peloton"

	// ErrPortMismatch represents port not matching.
	ErrPortMismatch = errors.New("port in launch not in offer")
	// ErrNotEnoughResource means resource is not enough to match given task.
	ErrNotEnoughResource = errors.New("not enough resources left to run task")
	// ErrNotEnoughRevocableResource means revocable resources is not enough to match given task
	ErrNotEnoughRevocableResource = errors.New("not enough revocable resources left to run task")
)

// Builder helps to build launchable Mesos TaskInfo from offers and
// peloton task configs.
type Builder struct {
	// A map from role -> scalar resources.
	scalars map[string]scalar.Resources

	revocable scalar.Resources

	// A map from available port number to role name.
	portToRoles map[uint32]string
}

// NewBuilder creates a new instance of Builder, which caller can use to
// build Mesos tasks from the cached resources.
func NewBuilder(
	resources []*mesos.Resource) *Builder {
	scalars := make(map[string]scalar.Resources)
	portToRoles := make(map[uint32]string)

	revocable, _ := scalar.FilterMesosResources(
		resources, func(r *mesos.Resource) bool {
			return r.GetRevocable() != nil
		})

	for _, rs := range resources {
		if rs.GetRevocable() != nil {
			continue
		}

		tmp := scalar.FromMesosResource(rs)
		role := rs.GetRole()

		if !tmp.Empty() {
			prev := scalars[role]
			scalars[role] = prev.Add(tmp)
		} else {

			ports := util.ExtractPortSet(rs)
			if len(ports) == 0 {
				continue
			}

			for p := range ports {
				portToRoles[p] = role
			}
		}
	}

	// TODO: Look into whether we need to prefer reserved (non-* role)
	// or unreserved (* role).
	return &Builder{
		scalars:     scalars,
		revocable:   scalar.FromMesosResources(revocable),
		portToRoles: portToRoles,
	}
}

// Helper struct for port picking result.
type portPickResult struct {
	// select ports numbers by name.
	// This is used to populated DiscoveryInfo.Ports
	// so service discovery systems can find the task.
	// This includes both static and dynamic ports.
	selectedPorts map[string]uint32

	// Environment variables. This includes both static and dynamic ports.
	portEnvs map[string]string

	// Mesos resources used. This will be passed within `TaskInfo` to Mesos
	// to launch the task.
	portResources []*mesos.Resource
}

// pickPorts inspects the given taskConfig, picks dynamic ports from
// available resources, and return selected static and dynamic ports
// as well as necessary environment variables and resources.
func (tb *Builder) pickPorts(
	taskConfig *task.TaskConfig,
	selectedDynamicPorts map[string]uint32) (*portPickResult, error) {

	result := &portPickResult{
		selectedPorts: make(map[string]uint32),
		portEnvs:      make(map[string]string),
		portResources: []*mesos.Resource{},
	}

	if len(taskConfig.Ports) == 0 {
		return result, nil
	}

	// Populates dynamic ports and build Mesos resources objects,
	// which will be used later to launch the task.
	dynamicPorts := make(map[uint32]string)
	for name, port := range selectedDynamicPorts {
		role, ok := tb.portToRoles[port]
		if !ok {
			return nil, ErrPortMismatch
		}
		delete(tb.portToRoles, port)
		dynamicPorts[port] = role
		result.selectedPorts[name] = port
	}

	result.portResources = append(
		result.portResources,
		util.CreatePortResources(dynamicPorts)...)

	// Populate static ports and extra environment variables, which will be
	// added to `CommandInfo` to launch the task.
	for _, portConfig := range taskConfig.GetPorts() {
		name := portConfig.GetName()
		if len(name) == 0 {
			return nil, errors.New("Empty port name in task")
		}
		value := portConfig.GetValue()
		if value != 0 { // static port
			result.selectedPorts[name] = value
		}

		if envName := portConfig.GetEnvName(); len(envName) == 0 {
			continue
		} else {
			p := int(result.selectedPorts[name])
			result.portEnvs[envName] = strconv.Itoa(p)
		}
	}

	return result, nil
}

// Build is used to build a `mesos.TaskInfo` from cached resources.
// Caller can keep calling this function to build more tasks.
// This returns error if the current
// instance does not have enough resources leftover (scalar, port in future),
// or the taskConfig is not correct.
func (tb *Builder) Build(
	task *hostsvc.LaunchableTask,
) (*mesos.TaskInfo, error) {

	// Validation of input.
	taskConfig := task.GetConfig()
	if taskConfig == nil {
		return nil, errors.New("TaskConfig cannot be nil")
	}

	taskID := task.GetTaskId()
	if taskID == nil {
		return nil, errors.New("taskID cannot be nil")
	}

	taskResources := taskConfig.Resource
	if taskResources == nil {
		return nil, errors.New("TaskConfig.Resource cannot be nil")
	}

	jobID, instanceID, err := util.ParseJobAndInstanceID(taskID.GetValue())
	if err != nil {
		return nil, err
	}

	if taskConfig.GetCommand() == nil {
		return nil, errors.New("Command cannot be nil")
	}

	// lres is list of "launch" resources this task needs when launched.
	lres, err := tb.extractScalarResources(
		taskConfig.GetResource(),
		taskConfig.GetRevocable())
	if err != nil {
		return nil, err
	}

	selectedDynamicPorts := task.GetPorts()
	pick, err := tb.pickPorts(taskConfig, selectedDynamicPorts)
	if err != nil {
		return nil, err
	}

	if len(pick.portResources) > 0 {
		lres = append(lres, pick.portResources...)
	}

	mesosTask := &mesos.TaskInfo{
		Name:      &jobID,
		TaskId:    taskID,
		Resources: lres,
	}

	tb.populateExecutorInfo(
		mesosTask,
		taskConfig.GetExecutor(),
		taskID,
	)
	tb.populateKillPolicy(mesosTask, taskConfig.GetKillGracePeriodSeconds())
	tb.populateDiscoveryInfo(mesosTask, pick.selectedPorts, jobID)
	tb.populateCommandInfo(
		mesosTask,
		taskConfig.GetCommand(),
		pick.portEnvs,
		jobID,
		instanceID,
	)
	tb.populateContainerInfo(mesosTask, taskConfig.GetContainer())
	tb.populateLabels(mesosTask, taskConfig.GetLabels(), jobID, instanceID)

	tb.populateHealthCheck(mesosTask, taskConfig.GetHealthCheck())

	return mesosTask, nil
}

// populateExecutorInfo sets up the ExecutorInfo of a Mesos task and copys
// executor data to Mesos task if present.
func (tb *Builder) populateExecutorInfo(
	mesosTask *mesos.TaskInfo,
	executor *mesos.ExecutorInfo,
	taskID *mesos.TaskID,
) {
	if executor == nil || executor.GetType() != mesos.ExecutorInfo_CUSTOM {
		// Only allow custom executor - aurora thermos - use case for now.
		// Maybe consider adding support for Mesos "default" executor in
		// the future??
		return
	}

	// Make a deep copy of pass through fields to avoid changing input.
	executorInfo := proto.Clone(executor).(*mesos.ExecutorInfo)

	executorIDValue := _defaultCustomExecutorPrefix + taskID.GetValue()
	executorInfo.ExecutorId = &mesos.ExecutorID{
		Value: &executorIDValue,
	}

	executorName := _defaultCustomExecutorName
	if executorInfo.Name == nil {
		executorInfo.Name = &executorName
	}

	// Not populating executor overhead resource, since it is assumed to be
	// included in the config provided by federation.
	executorInfo.Resources = []*mesos.Resource{}

	mesosTask.Executor = executorInfo

	// Make a copy of the executor data since aurora thermos executor
	// reads data from TaskInfo.
	if executorInfo.Data != nil {
		mesosTask.Data = make([]byte, len(executorInfo.Data))
		copy(mesosTask.Data, executorInfo.Data)
	}
}

// populateCommandInfo properly sets up the CommandInfo of a Mesos task
// and populates any optional environment variables in envMap. It populates
// ExecutorInfo field instead when custom executor is requested.
func (tb *Builder) populateCommandInfo(
	mesosTask *mesos.TaskInfo,
	command *mesos.CommandInfo,
	envMap map[string]string,
	jobID string,
	instanceID uint32,
) {

	if command == nil {
		// Input validation happens before this, so this is a case that
		//  we want a nil CommandInfo (aka custom executor case).
		return
	}

	// Make a deep copy of pass through fields to avoid changing input.
	commandInfo := proto.Clone(command).(*mesos.CommandInfo)

	// Populate optional environment variables.
	// Make sure `Environment` field is initialized.
	if commandInfo.GetEnvironment().GetVariables() == nil {
		commandInfo.Environment = &mesos.Environment{
			Variables: []*mesos.Environment_Variable{},
		}
	}

	pelotonEnvs := []*mesos.Environment_Variable{
		{
			Name:  util.PtrPrintf(hostmgrutil.LabelKeyToEnvVarName(PelotonJobIDLabelKey)),
			Value: &jobID,
		},
		{
			Name:  util.PtrPrintf(hostmgrutil.LabelKeyToEnvVarName(PelotonInstanceIDLabelKey)),
			Value: util.PtrPrintf("%d", instanceID),
		},
		{
			Name:  util.PtrPrintf(hostmgrutil.LabelKeyToEnvVarName(PelotonTaskIDLabelKey)),
			Value: util.PtrPrintf("%s-%d", jobID, instanceID),
		},
	}

	// Add peloton specific environtment variables.
	commandInfo.Environment.Variables = append(commandInfo.Environment.Variables, pelotonEnvs...)

	for name, value := range envMap {
		// Make a copy since taking address of key/value in map
		// is dangerous.
		tmpName := name
		tmpValue := value
		env := &mesos.Environment_Variable{
			Name:  &tmpName,
			Value: &tmpValue,
		}
		commandInfo.Environment.Variables =
			append(commandInfo.Environment.Variables, env)
	}

	if mesosTask.Executor != nil {
		// We are using custom executor
		mesosTask.Executor.Command = commandInfo
	} else {
		mesosTask.Command = commandInfo
	}
}

// populateContainerInfo properly sets up the `ContainerInfo` field of a task.
// It populates ContainerInfo if custom executor is requested.
func (tb *Builder) populateContainerInfo(
	mesosTask *mesos.TaskInfo,
	container *mesos.ContainerInfo,
) {
	if container == nil {
		return
	}

	// Make a deep copy to avoid changing input.
	containerInfo := proto.Clone(container).(*mesos.ContainerInfo)

	if mesosTask.Executor != nil {
		// We are using custom executor
		mesosTask.Executor.Container = containerInfo
	} else {
		mesosTask.Container = containerInfo
	}
}

// populateLabels properly sets up the `Labels` field of a mesos task.
func (tb *Builder) populateLabels(
	mesosTask *mesos.TaskInfo,
	labels []*peloton.Label,
	jobID string,
	instanceID uint32,
) {
	var mesosLabels *mesos.Labels

	// Translate all peloton task labels into mesos task labels
	if len(labels) == 0 {
		mesosLabels = &mesos.Labels{
			Labels: make([]*mesos.Label, 0, 3),
		}
	} else {
		mesosLabels = util.ConvertLabels(labels)
	}

	// add mesos task default labels
	mesosLabels.Labels = append(mesosLabels.Labels, &mesos.Label{
		Key:   util.PtrPrintf(PelotonJobIDLabelKey),
		Value: &jobID,
	})
	mesosLabels.Labels = append(mesosLabels.Labels, &mesos.Label{
		Key:   util.PtrPrintf(PelotonInstanceIDLabelKey),
		Value: util.PtrPrintf("%d", instanceID),
	})
	mesosLabels.Labels = append(mesosLabels.Labels, &mesos.Label{
		Key:   util.PtrPrintf(PelotonTaskIDLabelKey),
		Value: util.PtrPrintf("%s-%d", jobID, instanceID),
	})

	mesosTask.Labels = &mesos.Labels{
		Labels: mesosLabels.Labels,
	}
}

// populateKillPolicy populates the `KillPolicy` field of the task with the
// default or custom task kill grace period.
func (tb *Builder) populateKillPolicy(mesosTask *mesos.TaskInfo,
	taskKillGracePeriodSecs uint32) {
	// set grace period to default value
	gracePeriodNsec := int64(_defaultTaskKillGracePeriod.Nanoseconds())

	if taskKillGracePeriodSecs > 0 {
		gracePeriod := time.Duration(taskKillGracePeriodSecs) * time.Second
		gracePeriodNsec = int64(gracePeriod.Nanoseconds())
	}
	mesosTask.KillPolicy = &mesos.KillPolicy{
		GracePeriod: &mesos.DurationInfo{
			Nanoseconds: &gracePeriodNsec,
		},
	}
}

// populateDiscoveryInfo populates the `DiscoveryInfo` field of the task
// so service discovery integration can find which ports the task is using.
func (tb *Builder) populateDiscoveryInfo(
	mesosTask *mesos.TaskInfo,
	selectedPorts map[string]uint32,
	jobID string,
) {
	if len(selectedPorts) == 0 {
		return
	}

	// Visibility field on DiscoveryInfo is required.
	defaultVisibility := mesos.DiscoveryInfo_EXTERNAL
	portSlice := []*mesos.Port{}
	for name, value := range selectedPorts {
		// NOTE: we need tmp for both name and value,
		// as taking address during iterator is unsafe.
		tmpName := name
		tmpValue := value
		portSlice = append(portSlice, &mesos.Port{
			Name:       &tmpName,
			Number:     &tmpValue,
			Visibility: &defaultVisibility,
			// TODO: Consider add protocol, visibility and labels.
		})
	}

	// Note that this overrides any DiscoveryInfo even if it's in input.
	mesosTask.Discovery = &mesos.DiscoveryInfo{
		Name:       &jobID,
		Visibility: &defaultVisibility,
		Ports: &mesos.Ports{
			Ports: portSlice,
		},
		// TODO:
		// 1. add Environment, Location, Version, and Labels;
		// 2. Determine how to find this in bridge.
	}
}

// populateHealthCheck properly sets up the health check part of a Mesos task.
func (tb *Builder) populateHealthCheck(
	mesosTask *mesos.TaskInfo, health *task.HealthCheckConfig) {
	if health == nil {
		return
	}

	mh := &mesos.HealthCheck{}

	if t := health.GetInitialIntervalSecs(); t > 0 {
		tmp := float64(t)
		mh.DelaySeconds = &tmp
	}

	if t := health.GetIntervalSecs(); t > 0 {
		tmp := float64(t)
		mh.IntervalSeconds = &tmp
	}

	if t := health.GetTimeoutSecs(); t > 0 {
		tmp := float64(t)
		mh.TimeoutSeconds = &tmp
	}

	if t := health.GetMaxConsecutiveFailures(); t > 0 {
		tmp := uint32(t)
		mh.ConsecutiveFailures = &tmp
	}

	switch health.GetType() {
	case task.HealthCheckConfig_COMMAND:
		cc := health.GetCommandCheck()
		t := mesos.HealthCheck_COMMAND
		mh.Type = &t
		shell := true
		value := cc.GetCommand()
		cmd := &mesos.CommandInfo{
			Shell: &shell,
			Value: &value,
		}
		if !cc.GetUnshareEnvironments() {
			cmd.Environment = proto.Clone(
				mesosTask.GetCommand().GetEnvironment(),
			).(*mesos.Environment)
		}
		mh.Command = cmd
	case task.HealthCheckConfig_HTTP:
		cc := health.GetHttpCheck()
		t := mesos.HealthCheck_HTTP
		mh.Type = &t
		scheme := cc.GetScheme()
		port := cc.GetPort()
		path := cc.GetPath()
		h := &mesos.HealthCheck_HTTPCheckInfo{
			Scheme: &scheme,
			Port:   &port,
			Path:   &path,
		}
		mh.Http = h
	default:
		log.WithField("type", health.GetType()).
			Warn("Unknown health check type")
		return
	}

	log.WithFields(log.Fields{
		"health": mh,
		"task":   mesosTask.GetTaskId(),
	}).Debug("Populated health check for mesos task")
	mesosTask.HealthCheck = mh
}

// extractScalarResources takes necessary scalar resources from cached resources
// of this instance to construct a task, and returns error if not enough
// resources are left.
func (tb *Builder) extractScalarResources(
	taskResources *task.ResourceConfig,
	revocable bool) ([]*mesos.Resource, error) {
	var launchResources []*mesos.Resource
	requiredScalar := scalar.FromResourceConfig(taskResources)
	if revocable {
		// ToDo: Implement a more clean design on comparing revocable resources.
		rs, err := tb.extractRevocableScalarResources(&requiredScalar)
		if err != nil {
			log.WithFields(log.Fields{
				"resources_requested":       requiredScalar,
				"total_revocable_resources": tb.revocable,
			}).Error("not enough revocable resources to match current task")
			return nil, err
		}
		launchResources = append(launchResources, rs...)
	}

	for role, leftover := range tb.scalars {
		minimum := scalar.Minimum(leftover, requiredScalar)
		if minimum.Empty() {
			continue
		}
		rs := util.CreateMesosScalarResources(map[string]float64{
			common.MesosCPU:  minimum.CPU,
			common.MesosMem:  minimum.Mem,
			common.MesosDisk: minimum.Disk,
			common.MesosGPU:  minimum.GPU,
		}, role)

		launchResources = append(launchResources, rs...)

		trySubtract, ok := leftover.TrySubtract(minimum)
		if !ok {
			msg := "Incorrect resource amount in leftover!"
			log.WithFields(log.Fields{
				"leftover": leftover,
				"minimum":  minimum,
			}).Warn(msg)
			return nil, errors.New(msg)
		}
		leftover = trySubtract

		if leftover.Empty() {
			delete(tb.scalars, role)
		} else {
			tb.scalars[role] = leftover
		}

		trySubtract, ok = requiredScalar.TrySubtract(minimum)
		if !ok {
			msg := "Incorrect resource amount in required!"
			log.WithFields(log.Fields{
				"required": requiredScalar,
				"minimum":  minimum,
			}).Warn(msg)
			return nil, errors.New(msg)
		}
		requiredScalar = trySubtract

		if requiredScalar.Empty() {
			break
		}
	}

	if !requiredScalar.Empty() {
		log.WithFields(log.Fields{
			"task_resource_requested":       scalar.FromResourceConfig(taskResources).String(),
			"task_resources_left_to_match":  requiredScalar.String(),
			"total_revocable_resources":     tb.revocable.String(),
			"total_non_revocable_resources": tb.scalars,
		}).Error("task resources are still left to match")
		return nil, ErrNotEnoughResource
	}
	return launchResources, nil
}

// extractRevocableResources extract revocable resources for required task resources.
func (tb *Builder) extractRevocableScalarResources(
	requiredScalar *scalar.Resources) ([]*mesos.Resource, error) {
	leftover := math.Round((tb.revocable.CPU-requiredScalar.CPU)*100) / 100
	if leftover >= 0 {
		tb.revocable.CPU = leftover
	} else {
		return nil, ErrNotEnoughRevocableResource
	}

	rs := util.CreateMesosScalarResources(
		map[string]float64{common.MesosCPU: requiredScalar.CPU},
		"*")
	requiredScalar.CPU = 0
	for _, resource := range rs {
		resource.Revocable = &mesos.Resource_RevocableInfo{}
	}
	return rs, nil
}
