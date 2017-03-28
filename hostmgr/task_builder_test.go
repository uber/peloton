package hostmgr

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"

	mesos "mesos/v1"

	"peloton/api/task"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/util"
)

const (
	taskIDFmt  = "testjob-%d-abcdefgh-abcd-1234-5678-1234567890"
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

type TaskBuilderTestSuite struct {
	suite.Suite
}

// helper function for getting a set of ports from ranges of integers.
func (suite *TaskBuilderTestSuite) getPortSet(
	ranges ...uint32) map[uint32]bool {

	result := make(map[uint32]bool)
	begin := uint32(0)
	for index, num := range ranges {
		if index%2 == 0 {
			begin = num
		} else {
			for i := begin; i <= num; i++ {
				result[i] = true
			}
			begin = uint32(0)
		}
	}

	if begin != uint32(0) {
		panic("Odd number of input arguments!")
	}

	return result
}

// helper function to build resources which fits exactly numTasks
// default tasks.
func (suite *TaskBuilderTestSuite) getResources(
	numTasks int) []*mesos.Resource {

	resources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(float64(numTasks * _cpu)).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("mem").
			WithValue(float64(numTasks * _mem)).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("disk").
			WithValue(float64(numTasks * _disk)).
			Build(),
	}
	return resources
}

// helper function for creating test task ids
func (suite *TaskBuilderTestSuite) createTestTaskIDs(
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
	defaultResourceConfig = task.ResourceConfig{
		CpuLimit:    _cpu,
		MemLimitMb:  _mem,
		DiskLimitMb: _disk,
	}
)

// helper function to create a no-port task
func createTestTaskConfigs(numTasks int) []*task.TaskConfig {
	var configs []*task.TaskConfig
	tmpCmd := defaultCmd
	for i := 0; i < numTasks; i++ {
		configs = append(configs, &task.TaskConfig{
			Name:     fmt.Sprintf("name-%d", i),
			Resource: &defaultResourceConfig,
			Command: &mesos.CommandInfo{
				Value: &tmpCmd,
			},
		})
	}
	return configs
}

// helper function to test bi-directional port set/range transformation.
func (suite *TaskBuilderTestSuite) testPortSetTransformation(
	expected map[uint32]bool, rangeLen int) {

	ranges := createPortRanges(expected)
	suite.Equal(rangeLen, len(ranges.Range))

	rs := util.NewMesosResourceBuilder().
		WithName("ports").
		WithType(mesos.Value_RANGES).
		WithRanges(ranges).
		Build()
	portSet := extractPortSet(rs)
	suite.Equal(expected, portSet)
}

// This tests bidirectional transformation between set of available port and
// ranges in Mesos resource.
func (suite *TaskBuilderTestSuite) TestPortRanges() {
	// Empty range.
	rs := util.NewMesosResourceBuilder().
		WithName("ports").
		WithType(mesos.Value_RANGES).
		Build()
	portSet := extractPortSet(rs)
	suite.Empty(portSet)

	// Unmatch resource name.
	rs = util.NewMesosResourceBuilder().
		WithName("foo").
		WithType(mesos.Value_RANGES).
		Build()
	portSet = extractPortSet(rs)
	suite.Empty(portSet)

	// Empty one
	suite.testPortSetTransformation(suite.getPortSet(), 0)
	// Single port
	suite.testPortSetTransformation(suite.getPortSet(1000, 1000), 1)
	// Single range
	suite.testPortSetTransformation(suite.getPortSet(1000, 1001), 2)
	// Single range
	suite.testPortSetTransformation(suite.getPortSet(1000, 1003), 4)
	// Multiple ranges.
	suite.testPortSetTransformation(suite.getPortSet(1000, 1001, 1003, 1004), 4)

	// Multiple single ports.
	suite.testPortSetTransformation(
		suite.getPortSet(1000, 1000, 2000, 2000, 3000, 3000, 4000, 4000),
		4)

	// Multiple longer ranges.
	suite.testPortSetTransformation(
		suite.getPortSet(1000, 2000, 3000, 4000),
		2002)
	// Merged ranges.
	suite.testPortSetTransformation(
		suite.getPortSet(1000, 2000, 2001, 3000),
		2001)
	// More ranges
	suite.testPortSetTransformation(
		suite.getPortSet(1000, 1001, 2000, 2001, 3000, 3001, 4000, 4000),
		7)
}

// This tests several copies of simple tasks without any port can be
// created as long as there are enough resources.
func (suite *TaskBuilderTestSuite) TestNoPortTasks() {
	numTasks := 4
	resources := suite.getResources(numTasks)
	builder := newTaskBuilder(resources)
	suite.Equal(
		map[string]scalar.Resources{
			"*": {
				CPU:  float64(numTasks * _cpu),
				Mem:  float64(numTasks * _mem),
				Disk: float64(numTasks * _disk),
			},
		},
		builder.scalars)
	suite.Empty(builder.portSets)
	tids := suite.createTestTaskIDs(numTasks)
	configs := createTestTaskConfigs(numTasks)

	for i := 0; i < numTasks; i++ {
		info, err := builder.build(tids[i], configs[i])
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
	info, err := builder.build(tids[0], configs[0])
	suite.Nil(info)
	suite.EqualError(err, "Not enough resources left to run task")
}

// This tests several tasks requiring ports can be created.
func (suite *TaskBuilderTestSuite) TestPortTasks() {
	portSet := map[uint32]bool{
		1000: true,
		1002: true,
		1004: true,
		1006: true,
	}
	numTasks := 2
	// add more scalar resource to make sure we are only bound by ports.
	resourceTasks := 4
	resources := suite.getResources(resourceTasks)
	resources = append(resources,
		util.NewMesosResourceBuilder().
			WithName("ports").
			WithType(mesos.Value_RANGES).
			WithRanges(createPortRanges(portSet)).
			Build())

	builder := newTaskBuilder(resources)
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
		map[string]map[uint32]bool{"*": portSet},
		builder.portSets)
	tid := suite.createTestTaskIDs(1)[0]
	taskConfig := createTestTaskConfigs(1)[0]
	taskConfig.Ports = []*task.PortConfig{
		{
			Name:    "static",
			Value:   80,
			EnvName: "STATIC_PORT",
		},
		{ // dynamic with env name
			Name:    "dynamic_env",
			EnvName: "DYNAMIC_ENV_PORT",
		},
		{ // dynamic without env
			Name: "dynamic_no_env",
		},
	}
	tmpLabelKey := "label_key"
	tmpLabelValue := "label_value"
	taskConfig.Labels = &mesos.Labels{
		Labels: []*mesos.Label{
			{
				Key:   &tmpLabelKey,
				Value: &tmpLabelValue,
			},
		},
	}

	discoveryPortSet := make(map[uint32]bool)
	for i := 0; i < numTasks; i++ {
		info, err := builder.build(tid, taskConfig)
		suite.NoError(err)
		suite.Equal(tid, info.GetTaskId())
		sc := scalar.FromMesosResources(info.GetResources())
		suite.Equal(
			scalar.Resources{CPU: _cpu, Mem: _mem, Disk: _disk},
			sc)
		discoveryInfo := info.GetDiscovery()
		suite.NotNil(discoveryInfo)
		suite.Equal("testjob", discoveryInfo.GetName()) // job id
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
		suite.Equal(2, len(envVars))

		envMap := make(map[string]string)
		for _, envVar := range envVars {
			envMap[envVar.GetName()] = envVar.GetValue()
		}
		suite.Equal(2, len(envMap))
		suite.Contains(envMap, "DYNAMIC_ENV_PORT")
		p, err := strconv.Atoi(envMap["DYNAMIC_ENV_PORT"])
		suite.NoError(err)
		suite.Contains(portSet, uint32(p))
		suite.Contains(envMap, "STATIC_PORT")
		suite.Equal("80", envMap["STATIC_PORT"])

		suite.Equal(
			strconv.Itoa(int(portsInDiscovery["dynamic_env"])),
			envMap["DYNAMIC_ENV_PORT"])

		suite.Equal(taskConfig.Labels, info.Labels)
	}

	suite.Equal(portSet, discoveryPortSet)

	// next build call will return an error due to insufficient ports.
	info, err := builder.build(tid, taskConfig)
	suite.Nil(info)
	suite.EqualError(err, errNotEnoughPorts.Error())
}

// This tests task with command health can be created.
func (suite *TaskBuilderTestSuite) TestCommandHealthCheck() {
	numTasks := 1
	resources := suite.getResources(numTasks)
	builder := newTaskBuilder(resources)
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
	info, err := builder.build(tid, c)
	suite.NoError(err)
	suite.Equal(tid, info.GetTaskId())
	hc := info.GetHealthCheck().GetCommand()
	suite.NotNil(hc)
	suite.Equal(hcCmd, hc.GetValue())
	suite.True(hc.GetShell())
	suite.Empty(hc.GetEnvironment().GetVariables())
}

// This tests various combination of populating health check.
func (suite *TaskBuilderTestSuite) TestPopulateHealthCheck() {
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
		builder  *taskBuilder
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
			builder = newTaskBuilder(empty)
		}
		taskInfo := tt.taskInfo
		if taskInfo == nil {
			taskInfo = &mesos.TaskInfo{}
		}
		builder.populateHealthCheck(taskInfo, tt.input)
		suite.Equal(tt.output, taskInfo.GetHealthCheck())
	}
}

func TestTaskBuilderTestSuite(t *testing.T) {
	suite.Run(t, new(TaskBuilderTestSuite))
}
