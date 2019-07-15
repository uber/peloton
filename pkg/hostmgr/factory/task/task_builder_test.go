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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	mesos "github.com/uber/peloton/.gen/mesos/v1"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	hostmgrutil "github.com/uber/peloton/pkg/hostmgr/util"
)

const (
	_testJobID = "bca875f5-322a-4439-b0c9-63e3cf9f982e"
	taskIDFmt  = _testJobID + "-%d-abcdef12-abcd-1234-5678-1234567890ab"
	defaultCmd = "/bin/sh"
)

// TODO: add following test cases:
// - sufficient resources for single task;
// - insufficient resources for single task;
// - sufficient resources for multiple tasks;
// - insufficient resources for any of multiple tasks;
// - insufficient resources for partial of multiple tasks;

// TODO: after port picking, add following test cases:
// - sufficient scalar resources for multiple tasks w/ multiple ports, some exports to envs while some doesn't;
// - sufficient scalar resources for multiple tasks but insufficient multiple ports;

type BuilderTestSuite struct {
	suite.Suite
}

// helper function to build resources which fits exactly numTasks
// default tasks.
func (suite *BuilderTestSuite) getResources(
	numTasks int) []*mesos.Resource {

	resources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(float64(numTasks * _cpu)).
			WithRole("*").
			Build(),
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(float64(numTasks * _cpu)).
			WithRevocable(&mesos.Resource_RevocableInfo{}).
			WithRole("*").
			Build(),
		util.NewMesosResourceBuilder().
			WithName("mem").
			WithValue(float64(numTasks * _mem)).
			WithRole("*").
			Build(),
		util.NewMesosResourceBuilder().
			WithName("disk").
			WithValue(float64(numTasks * _disk)).
			Build(),
	}
	return resources
}

// helper function for creating test task ids
func (suite *BuilderTestSuite) createTestTaskIDs(
	numTasks int) []*mesos.TaskID {

	var tids []*mesos.TaskID
	for i := 0; i < numTasks; i++ {
		tmp := fmt.Sprintf(taskIDFmt, i)
		tids = append(tids, &mesos.TaskID{Value: &tmp})
	}
	return tids
}

const (
	_cpu  = 10
	_mem  = 20
	_disk = 30
)

var (
	_defaultResourceConfig = task.ResourceConfig{
		CpuLimit:    _cpu,
		MemLimitMb:  _mem,
		DiskLimitMb: _disk,
	}

	_tmpLabelKey   = "label_key"
	_tmpLabelValue = "label_value"
)

// helper function to create a no-port task
func createTestTaskConfigs(numTasks int) []*task.TaskConfig {
	var configs []*task.TaskConfig
	tmpCmd := defaultCmd
	for i := 0; i < numTasks; i++ {
		configs = append(configs, &task.TaskConfig{
			Name:     fmt.Sprintf("name-%d", i),
			Resource: &_defaultResourceConfig,
			Command: &mesos.CommandInfo{
				Value: &tmpCmd,
			},
		})
	}
	return configs
}

func (suite *BuilderTestSuite) TestRevocableFloatOverflow() {
	resources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(float64(19)).
			WithRevocable(&mesos.Resource_RevocableInfo{}).
			WithRole("*").
			Build(),
	}
	builder := NewBuilder(resources)
	for i := 0; i < 190; i++ {
		requiredScalar := &scalar.Resources{
			CPU: 0.1,
		}
		_, err := builder.extractRevocableScalarResources(requiredScalar)
		suite.NoError(err)
	}
}

// This tests several copies of simple tasks without any port can be
// created as long as there are enough resources.
func (suite *BuilderTestSuite) TestNoPortTasks() {
	numTasks := 4
	resources := suite.getResources(numTasks)
	builder := NewBuilder(resources)
	suite.Equal(
		map[string]scalar.Resources{
			"*": {
				CPU:  float64(numTasks * _cpu),
				Mem:  float64(numTasks * _mem),
				Disk: float64(numTasks * _disk),
			},
		},
		builder.scalars)
	suite.Empty(builder.portToRoles)
	tids := suite.createTestTaskIDs(numTasks)
	configs := createTestTaskConfigs(numTasks)

	for i := 0; i < numTasks; i++ {
		task := &hostsvc.LaunchableTask{
			TaskId: tids[i],
			Config: configs[i],
			Ports:  nil,
		}
		info, err := builder.Build(task)
		suite.NoError(err)
		suite.Equal(tids[i], info.GetTaskId())
		sc := scalar.FromMesosResources(info.GetResources())
		suite.Equal(
			scalar.Resources{
				CPU:  _cpu,
				Mem:  _mem,
				Disk: _disk,
			},
			sc)
	}

	// next build call will return an error due to insufficient resource.
	task := &hostsvc.LaunchableTask{
		TaskId: tids[0],
		Config: configs[0],
		Ports:  nil,
	}
	info, err := builder.Build(task)
	suite.Nil(info)
	suite.Equal(err, ErrNotEnoughResource)
}

// This tests several tasks requiring ports can be created.
func (suite *BuilderTestSuite) TestPortTasks() {
	portToRole := map[uint32]string{
		1000: "*",
		1002: "*",
		1004: "role",
		1006: "role",
	}

	numTasks := 2
	// add more scalar resource to make sure we are only bound by ports.
	resourceTasks := 4
	resources := suite.getResources(resourceTasks)
	resources = append(resources, util.CreatePortResources(portToRole)...)

	builder := NewBuilder(resources)
	suite.Equal(
		map[string]scalar.Resources{
			"*": {
				CPU:  float64(resourceTasks * _cpu),
				Mem:  float64(resourceTasks * _mem),
				Disk: float64(resourceTasks * _disk),
			},
		},
		builder.scalars)
	suite.Equal(
		map[uint32]string{
			1000: "*",
			1002: "*",
			1004: "role",
			1006: "role",
		},
		builder.portToRoles)
	tid := suite.createTestTaskIDs(2)
	taskConfig := createTestTaskConfigs(1)[0]
	// Requires 3 ports, 1 static and 2 dynamic ones.
	taskConfig.Ports = []*task.PortConfig{
		{
			Name:    "static",
			Value:   80,
			EnvName: "STATIC_PORT",
		},
		{
			Name:    "dynamic_env",
			EnvName: "DYNAMIC_ENV",
		},
		{
			Name:    "dynamic_env_port",
			EnvName: "DYNAMIC_ENV_PORT",
		},
	}
	taskConfig.Labels = []*peloton.Label{
		{
			Key:   _tmpLabelKey,
			Value: _tmpLabelValue,
		},
	}
	selectedDynamicPorts := []map[string]uint32{
		{
			"dynamic_env":      1000,
			"dynamic_env_port": 1002,
		},
		{
			"dynamic_env":      1004,
			"dynamic_env_port": 1006,
		},
	}

	discoveryPortSet := make(map[uint32]bool)
	for i := 0; i < numTasks; i++ {
		task := &hostsvc.LaunchableTask{
			TaskId: tid[i],
			Config: taskConfig,
			Ports:  selectedDynamicPorts[i],
		}
		info, err := builder.Build(task)
		suite.NoError(err)
		suite.Equal(tid[i], info.GetTaskId())
		sc := scalar.FromMesosResources(info.GetResources())
		suite.Equal(
			scalar.Resources{CPU: _cpu, Mem: _mem, Disk: _disk},
			sc)
		discoveryInfo := info.GetDiscovery()
		suite.NotNil(discoveryInfo)
		suite.Equal(_testJobID, discoveryInfo.GetName()) // job id
		mesosPorts := discoveryInfo.GetPorts().GetPorts()
		suite.Equal(3, len(mesosPorts))

		var staticFound uint32
		portsInDiscovery := make(map[string]uint32)
		for _, mp := range mesosPorts {
			if mp.GetName() == "static" {
				staticFound = mp.GetNumber()
			} else {
				portsInDiscovery[mp.GetName()] = mp.GetNumber()
				discoveryPortSet[mp.GetNumber()] = true
			}
		}

		suite.Equal(
			uint32(80),
			staticFound,
			"static port is not found in %v", mesosPorts)

		envVars := info.GetCommand().GetEnvironment().GetVariables()
		suite.Equal(6, len(envVars))

		envMap := make(map[string]string)
		for _, envVar := range envVars {
			envMap[envVar.GetName()] = envVar.GetValue()
		}
		suite.Equal(6, len(envMap))
		suite.Contains(envMap, "DYNAMIC_ENV_PORT")
		p, err := strconv.Atoi(envMap["DYNAMIC_ENV_PORT"])
		suite.NoError(err)
		suite.Contains(portToRole, uint32(p))
		suite.Contains(envMap, "STATIC_PORT")
		suite.Equal("80", envMap["STATIC_PORT"])

		suite.Equal(
			strconv.Itoa(int(portsInDiscovery["dynamic_env"])),
			envMap["DYNAMIC_ENV"])

		suite.Equal(
			_testJobID,
			envMap[hostmgrutil.LabelKeyToEnvVarName(PelotonJobIDLabelKey)],
		)
		suite.Equal(
			fmt.Sprint(i),
			envMap[hostmgrutil.LabelKeyToEnvVarName(PelotonInstanceIDLabelKey)],
		)
		suite.Equal(
			fmt.Sprint(_testJobID+"-", i),
			envMap[hostmgrutil.LabelKeyToEnvVarName(PelotonTaskIDLabelKey)],
		)

		suite.Equal(&mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_tmpLabelKey,
					Value: &_tmpLabelValue,
				},
				{
					Key:   util.PtrPrintf(PelotonJobIDLabelKey),
					Value: util.PtrPrintf(_testJobID),
				},
				{
					Key:   util.PtrPrintf(PelotonInstanceIDLabelKey),
					Value: util.PtrPrintf(fmt.Sprint(i)),
				},
				{
					Key:   util.PtrPrintf(PelotonTaskIDLabelKey),
					Value: util.PtrPrintf(fmt.Sprint(_testJobID+"-", i)),
				},
			},
		}, info.Labels)
	}

	suite.Len(discoveryPortSet, 4)
}

// TestBuilderPickPorts tests pickPorts call and its return value.
func (suite *BuilderTestSuite) TestBuilderPickPorts() {
	portSet := map[uint32]bool{
		1000: true,
		1002: true,
		1004: true,
		1006: true,
	}
	// add more scalar resource to make sure we are only bound by ports.
	resourceTasks := 4
	resources := suite.getResources(resourceTasks)
	resources = append(resources,
		util.NewMesosResourceBuilder().
			WithName("ports").
			WithType(mesos.Value_RANGES).
			WithRanges(util.CreatePortRanges(portSet)).
			Build())

	builder := NewBuilder(resources)
	taskConfig := createTestTaskConfigs(1)[0]
	// Requires 3 ports, 1 static and 2 dynamic ones.
	taskConfig.Ports = []*task.PortConfig{
		{
			Name:  "static",
			Value: 80,
		},
		{
			Name:    "dynamic_env",
			EnvName: "DYNAMIC_ENV",
		},
		{
			Name:    "dynamic_env_port",
			EnvName: "DYNAMIC_ENV_PORT",
		},
	}
	selectedDynamicPorts := map[string]uint32{
		"dynamic_env":      1000,
		"dynamic_env_port": 1002,
	}

	result, err := builder.pickPorts(taskConfig, selectedDynamicPorts)
	suite.NoError(err)
	suite.Equal(len(result.selectedPorts), 3)
	suite.Equal(len(result.portEnvs), 2)
	suite.Equal(len(result.portResources), 2)
}

// This tests task with command health can be created.
func (suite *BuilderTestSuite) TestCommandHealthCheck() {
	numTasks := 1
	resources := suite.getResources(numTasks)
	builder := NewBuilder(resources)
	tid := suite.createTestTaskIDs(numTasks)[0]
	c := createTestTaskConfigs(numTasks)[0]

	hcCmd := "hello world"
	cmdCfg := &task.HealthCheckConfig_CommandCheck{
		Command: hcCmd,
	}
	c.HealthCheck = &task.HealthCheckConfig{
		Type:         task.HealthCheckConfig_COMMAND,
		CommandCheck: cmdCfg,
	}
	task := &hostsvc.LaunchableTask{
		TaskId: tid,
		Config: c,
		Ports:  nil,
	}
	info, err := builder.Build(task)
	suite.NoError(err)
	suite.Equal(tid, info.GetTaskId())
	hc := info.GetHealthCheck().GetCommand()
	suite.NotNil(hc)
	suite.Equal(hcCmd, hc.GetValue())
	suite.True(hc.GetShell())
	suite.Len(hc.GetEnvironment().GetVariables(), 3)
}

// This tests task with http health can be created.
func (suite *BuilderTestSuite) TestHTTPHealthCheck() {
	numTasks := 1
	resources := suite.getResources(numTasks)
	builder := NewBuilder(resources)
	tid := suite.createTestTaskIDs(numTasks)[0]
	c := createTestTaskConfigs(numTasks)[0]

	scheme := "http"
	port := uint32(100)
	path := "/health"
	httpCfg := &task.HealthCheckConfig_HTTPCheck{
		Scheme: scheme,
		Port:   port,
		Path:   path,
	}
	c.HealthCheck = &task.HealthCheckConfig{
		Type:      task.HealthCheckConfig_HTTP,
		HttpCheck: httpCfg,
	}
	task := &hostsvc.LaunchableTask{
		TaskId: tid,
		Config: c,
		Ports:  nil,
	}
	info, err := builder.Build(task)
	suite.NoError(err)
	suite.Equal(tid, info.GetTaskId())
	hc := info.GetHealthCheck().GetHttp()
	suite.NotNil(hc)
	suite.Equal(scheme, hc.GetScheme())
	suite.Equal(port, hc.GetPort())
	suite.Equal(path, hc.GetPath())
}

func (suite *BuilderTestSuite) TestRevocableTask() {
	numTasks := 1
	resources := suite.getResources(numTasks)
	builder := NewBuilder(resources)
	tid := suite.createTestTaskIDs(numTasks)[0]
	c := createTestTaskConfigs(numTasks)[0]
	c.Revocable = true

	hcCmd := "hello world"
	cmdCfg := &task.HealthCheckConfig_CommandCheck{
		Command: hcCmd,
	}
	c.HealthCheck = &task.HealthCheckConfig{
		Type:         task.HealthCheckConfig_COMMAND,
		CommandCheck: cmdCfg,
	}
	task := &hostsvc.LaunchableTask{
		TaskId: tid,
		Config: c,
		Ports:  nil,
	}
	info, err := builder.Build(task)
	suite.NoError(err)
	suite.Equal(tid, info.GetTaskId())
	hc := info.GetHealthCheck().GetCommand()
	suite.NotNil(hc)
	suite.Equal(hcCmd, hc.GetValue())
	suite.True(hc.GetShell())
	suite.Len(hc.GetEnvironment().GetVariables(), 3)

	// Revocable resources are not sufficient
	builder = NewBuilder(nil)
	_, err = builder.Build(task)
	suite.Error(err)

	task.Config.Command = nil
	_, err = builder.Build(task)
	suite.Error(err)

	task.Config.Resource = nil
	_, err = builder.Build(task)
	suite.Error(err)

	task.TaskId = nil
	_, err = builder.Build(task)
	suite.Error(err)

	task.Config = nil
	_, err = builder.Build(task)
	suite.Error(err)
}

// This tests various combination of populating health check.
func (suite *BuilderTestSuite) TestPopulateHealthCheck() {
	cmdType := mesos.HealthCheck_COMMAND
	command := "hello world"
	tmpTrue := true

	delaySeconds := float64(1)
	timeoutSeconds := float64(2)
	intervalSeconds := float64(3)
	consecutiveFailures := uint32(4)

	envName := "name"
	envValue := "value"
	environment := &mesos.Environment{
		Variables: []*mesos.Environment_Variable{
			{
				Name:  &envName,
				Value: &envValue,
			},
		},
	}

	var testCases = []struct {
		builder  *Builder
		taskInfo *mesos.TaskInfo
		input    *task.HealthCheckConfig
		output   *mesos.HealthCheck
		err      error
	}{
		// default values
		{
			input: &task.HealthCheckConfig{
				Type: task.HealthCheckConfig_COMMAND,
				CommandCheck: &task.HealthCheckConfig_CommandCheck{
					Command: command,
				},
			},
			output: &mesos.HealthCheck{
				Type: &cmdType,
				Command: &mesos.CommandInfo{
					Shell: &tmpTrue,
					Value: &command,
				},
			},
		},
		// custom values w/ environment variables.
		{
			input: &task.HealthCheckConfig{
				Type: task.HealthCheckConfig_COMMAND,
				CommandCheck: &task.HealthCheckConfig_CommandCheck{
					Command: command,
				},
				InitialIntervalSecs:    uint32(delaySeconds),
				TimeoutSecs:            uint32(timeoutSeconds),
				IntervalSecs:           uint32(intervalSeconds),
				MaxConsecutiveFailures: uint32(consecutiveFailures),
			},
			output: &mesos.HealthCheck{
				Type: &cmdType,
				Command: &mesos.CommandInfo{
					Shell:       &tmpTrue,
					Value:       &command,
					Environment: environment,
				},
				ConsecutiveFailures: &consecutiveFailures,
				DelaySeconds:        &delaySeconds,
				TimeoutSeconds:      &timeoutSeconds,
				IntervalSeconds:     &intervalSeconds,
			},
			taskInfo: &mesos.TaskInfo{
				Command: &mesos.CommandInfo{
					Environment: environment,
				},
			},
		},
		// unshare environment variables from task info
		{
			input: &task.HealthCheckConfig{
				Type: task.HealthCheckConfig_COMMAND,
				CommandCheck: &task.HealthCheckConfig_CommandCheck{
					Command:             command,
					UnshareEnvironments: true,
				},
			},
			output: &mesos.HealthCheck{
				Type: &cmdType,
				Command: &mesos.CommandInfo{
					Shell: &tmpTrue,
					Value: &command,
				},
			},
			taskInfo: &mesos.TaskInfo{
				Command: &mesos.CommandInfo{
					Environment: environment,
				},
			},
		},
	}

	for _, tt := range testCases {
		builder := tt.builder
		if builder == nil {
			var empty []*mesos.Resource
			builder = NewBuilder(empty)
		}
		taskInfo := tt.taskInfo
		if taskInfo == nil {
			taskInfo = &mesos.TaskInfo{}
		}
		builder.populateHealthCheck(taskInfo, tt.input)
		suite.Equal(tt.output, taskInfo.GetHealthCheck())
	}
}

// TestPopulateLabels tests populateLabels.
func (suite *BuilderTestSuite) TestPopulateLabels() {
	jobID := "test-job"
	instanceID := 0
	resources := suite.getResources(instanceID)
	builder := NewBuilder(resources)

	tt := []struct {
		pelotonTaskLabels []*peloton.Label // input labels
		mesosTaskLabels   *mesos.Labels    // expected output labels
	}{
		{
			// No peloton task label
			pelotonTaskLabels: []*peloton.Label{},
			mesosTaskLabels: &mesos.Labels{
				Labels: []*mesos.Label{
					{
						Key:   util.PtrPrintf(PelotonJobIDLabelKey),
						Value: util.PtrPrintf("test-job"),
					},
					{
						Key:   util.PtrPrintf(PelotonInstanceIDLabelKey),
						Value: util.PtrPrintf("0"),
					},
					{
						Key:   util.PtrPrintf(PelotonTaskIDLabelKey),
						Value: util.PtrPrintf("test-job-0"),
					},
				},
			},
		}, {
			// Some peloton task labels
			pelotonTaskLabels: []*peloton.Label{
				{
					Key:   _tmpLabelKey,
					Value: _tmpLabelValue,
				},
			},
			mesosTaskLabels: &mesos.Labels{
				Labels: []*mesos.Label{
					{
						Key:   &_tmpLabelKey,
						Value: &_tmpLabelValue,
					},
					{
						Key:   util.PtrPrintf(PelotonJobIDLabelKey),
						Value: util.PtrPrintf("test-job"),
					},
					{
						Key:   util.PtrPrintf(PelotonInstanceIDLabelKey),
						Value: util.PtrPrintf("0"),
					},
					{
						Key:   util.PtrPrintf(PelotonTaskIDLabelKey),
						Value: util.PtrPrintf("test-job-0"),
					},
				},
			},
		},
	}

	for _, test := range tt {
		mesosTask := &mesos.TaskInfo{}
		builder.populateLabels(
			mesosTask, test.pelotonTaskLabels, jobID, uint32(instanceID))
		suite.Equal(test.mesosTaskLabels, mesosTask.Labels)
	}
}

// TestPopulateKillPolicy tests setting the kill policy of tasks with a grace
// period
func (suite *BuilderTestSuite) TestPopulateKillPolicy() {
	numTasks := 1
	resources := suite.getResources(numTasks)
	builder := NewBuilder(resources)
	taskConfig := createTestTaskConfigs(numTasks)[0]

	mesosTask := &mesos.TaskInfo{}
	builder.populateKillPolicy(
		mesosTask, taskConfig.GetKillGracePeriodSeconds())

	// Verify default grace period
	suite.Equal(mesosTask.GetKillPolicy().GetGracePeriod().GetNanoseconds(),
		_defaultTaskKillGracePeriod.Nanoseconds())

	taskConfig.KillGracePeriodSeconds = 100
	builder.populateKillPolicy(
		mesosTask, taskConfig.GetKillGracePeriodSeconds())

	expectedGracePeriod := 100 * time.Second
	suite.Equal(mesosTask.GetKillPolicy().GetGracePeriod().GetNanoseconds(),
		expectedGracePeriod.Nanoseconds())
}

// TestPopulateExecutorInfo tests setting the executor info of tasks.
func (suite *BuilderTestSuite) TestPopulateExecutorInfo() {
	numTasks := 1
	resources := suite.getResources(numTasks)
	builder := NewBuilder(resources)
	taskIDValue := "test-task-id"
	taskID := &mesos.TaskID{
		Value: &taskIDValue,
	}
	executorType := mesos.ExecutorInfo_CUSTOM
	executorData := []byte{1, 2, 3, 4, 5}
	executor := &mesos.ExecutorInfo{
		Type: &executorType,
		Data: executorData,
	}

	mesosTask := &mesos.TaskInfo{}
	builder.populateExecutorInfo(
		mesosTask,
		executor,
		taskID,
	)

	suite.Equal(mesos.ExecutorInfo_CUSTOM, mesosTask.GetExecutor().GetType())
	suite.Equal("thermos-"+taskIDValue, mesosTask.GetExecutor().GetExecutorId().GetValue())
	suite.Equal("AuroraExecutor", mesosTask.GetExecutor().GetName())
	suite.Equal([]*mesos.Resource{}, mesosTask.GetExecutor().GetResources())
	suite.Equal(executorData, mesosTask.GetData())
}

// TestExtractScalarResources tests extracting task resources from cached
// host resources, and verifies extracted and remaining values are correct.
func (suite *BuilderTestSuite) TestExtractScalarResources() {
	numTasks := 1
	resources := suite.getResources(numTasks)
	builder := NewBuilder(resources)

	taskResources := &task.ResourceConfig{
		CpuLimit:    5,
		MemLimitMb:  10,
		DiskLimitMb: 7,
	}

	taskRes, err := builder.extractScalarResources(
		taskResources,
		false)
	suite.NoError(err)

	suite.Equal(3, len(taskRes))

	for _, res := range taskRes {
		switch resName := res.GetName(); resName {
		case "cpus":
			suite.Equal(taskResources.CpuLimit, res.GetScalar().GetValue())
		case "mem":
			suite.Equal(taskResources.MemLimitMb, res.GetScalar().GetValue())
		case "disk":
			suite.Equal(taskResources.DiskLimitMb, res.GetScalar().GetValue())
		default:
			suite.Failf("Unexpected resource", "Unexpected resource %s", resName)
		}
	}

	suite.Equal(float64(_cpu-5), builder.scalars["*"].CPU)
	suite.Equal(float64(_mem-10), builder.scalars["*"].Mem)
	suite.Equal(float64(_disk-7), builder.scalars["*"].Disk)
	suite.Equal(float64(0), builder.scalars["*"].GPU)

	suite.Equal(float64(_cpu), builder.revocable.CPU)
	suite.Equal(float64(0), builder.revocable.Mem)
	suite.Equal(float64(0), builder.revocable.Disk)
	suite.Equal(float64(0), builder.revocable.GPU)
}

// TestExtractScalarResourcesRevocable tests extracting revocable task
// resources from cached host resources, and verifies extracted and
// remaining values are correct.
func (suite *BuilderTestSuite) TestExtractScalarResourcesRevocable() {
	resources := suite.getResources(1)
	builder := NewBuilder(resources)

	taskResources := &task.ResourceConfig{
		CpuLimit:    6,
		MemLimitMb:  10,
		DiskLimitMb: 7,
	}

	taskRes, err := builder.extractScalarResources(
		taskResources,
		true)
	suite.NoError(err)

	suite.Equal(3, len(taskRes))

	for _, res := range taskRes {
		switch resName := res.GetName(); resName {
		case "cpus":
			suite.Equal(taskResources.CpuLimit, res.GetScalar().GetValue())
			suite.NotNil(res.GetRevocable())
		case "mem":
			suite.Equal(taskResources.MemLimitMb, res.GetScalar().GetValue())
			suite.Nil(res.GetRevocable())
		case "disk":
			suite.Equal(taskResources.DiskLimitMb, res.GetScalar().GetValue())
			suite.Nil(res.GetRevocable())
		default:
			suite.Failf("Unexpected resource", "Unexpected resource %s", resName)
		}
	}

	suite.Equal(float64(_cpu), builder.scalars["*"].CPU)
	suite.Equal(float64(_mem-10), builder.scalars["*"].Mem)
	suite.Equal(float64(_disk-7), builder.scalars["*"].Disk)
	suite.Equal(float64(0), builder.scalars["*"].GPU)

	suite.Equal(float64(_cpu-6), builder.revocable.CPU)
	suite.Equal(float64(0), builder.revocable.Mem)
	suite.Equal(float64(0), builder.revocable.Disk)
	suite.Equal(float64(0), builder.revocable.GPU)
}

// TestExtractScalarResourcesOverflow tests extracting task resources
// exceeding host resources, and verifies an error will be returned.
func (suite *BuilderTestSuite) TestExtractScalarResourcesOverflow() {
	resources := suite.getResources(1)
	builder := NewBuilder(resources)

	taskResources := &task.ResourceConfig{
		CpuLimit:    11,
		MemLimitMb:  10,
		DiskLimitMb: 7,
	}

	_, err := builder.extractScalarResources(
		taskResources,
		false)
	suite.Error(err)
}

func TestBuilderTestSuite(t *testing.T) {
	suite.Run(t, new(BuilderTestSuite))
}
