package task

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/util"
)

const (
	taskIDFmt  = "testjob-%d-abcdef12-abcd-1234-5678-1234567890ab"
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
		info, err := builder.Build(tids[i], configs[i], nil, nil, nil)
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
	info, err := builder.Build(tids[0], configs[0], nil, nil, nil)
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
		info, err := builder.Build(
			tid[i], taskConfig, selectedDynamicPorts[i], nil, nil)
		suite.NoError(err)
		suite.Equal(tid[i], info.GetTaskId())
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

		suite.Equal("testjob", envMap[PelotonJobID])
		suite.Equal(fmt.Sprint(i), envMap[PelotonInstanceID])
		suite.Equal(fmt.Sprint("testjob-", i), envMap[PelotonTaskID])

		suite.Equal(&mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_tmpLabelKey,
					Value: &_tmpLabelValue,
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
	info, err := builder.Build(tid, c, nil, nil, nil)
	suite.NoError(err)
	suite.Equal(tid, info.GetTaskId())
	hc := info.GetHealthCheck().GetCommand()
	suite.NotNil(hc)
	suite.Equal(hcCmd, hc.GetValue())
	suite.True(hc.GetShell())
	suite.Len(hc.GetEnvironment().GetVariables(), 3)
}

// This tests various combination of populating health check.
func (suite *BuilderTestSuite) TestPopulateHealthCheck() {
	cmdType := mesos.HealthCheck_COMMAND
	command := "hello world"
	tmpTrue := true

	delaySeconds := float64(1)
	timeoutSeconds := float64(2)
	intervalSeconds := float64(3)
	gracePeriodSeconds := float64(4)

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
				ConsecutiveFailures: &[]uint32{math.MaxUint32}[0],
				GracePeriodSeconds:  &[]float64{0.0}[0],
			},
		},
		// custom values w/ environment variables.
		{
			input: &task.HealthCheckConfig{
				Type: task.HealthCheckConfig_COMMAND,
				CommandCheck: &task.HealthCheckConfig_CommandCheck{
					Command: command,
				},
				InitialIntervalSecs: uint32(delaySeconds),
				TimeoutSecs:         uint32(timeoutSeconds),
				IntervalSecs:        uint32(intervalSeconds),
				GracePeriodSecs:     uint32(gracePeriodSeconds),
			},
			output: &mesos.HealthCheck{
				Type: &cmdType,
				Command: &mesos.CommandInfo{
					Shell:       &tmpTrue,
					Value:       &command,
					Environment: environment,
				},
				DelaySeconds:        &delaySeconds,
				TimeoutSeconds:      &timeoutSeconds,
				IntervalSeconds:     &intervalSeconds,
				GracePeriodSeconds:  &gracePeriodSeconds,
				ConsecutiveFailures: &[]uint32{math.MaxUint32}[0],
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
				ConsecutiveFailures: &[]uint32{math.MaxUint32}[0],
				GracePeriodSeconds:  &[]float64{0.0}[0],
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

// TestPopulateReservationVolumeInfo tests populateReservationInfo.
func (suite *BuilderTestSuite) TestPopulateReservationVolumeInfo() {
	numTasks := 1
	resources := suite.getResources(numTasks)
	labels := &mesos.Labels{
		Labels: []*mesos.Label{
			{
				Key:   &_tmpLabelKey,
				Value: &_tmpLabelValue,
			},
		},
	}

	testContainerPath := "testContainerPath"
	testVolumeID := "testVolumeID"
	volumeInfo := &hostsvc.Volume{
		Id: &peloton.VolumeID{
			Value: testVolumeID,
		},
		ContainerPath: testContainerPath,
		Resource: util.NewMesosResourceBuilder().
			WithName("disk").
			WithValue(float64(3 * numTasks * _disk)).
			Build(),
	}
	var err error
	resources, err = populateReservationVolumeInfo(resources, labels, volumeInfo)
	suite.NoError(err)

	var sandboxDiskRes, persistentDiskRes *mesos.Resource
	for _, res := range resources {
		suite.Equal(res.GetRole(), _pelotonRole)
		suite.Equal(res.GetReservation().GetLabels(), labels)
		if res.GetName() == "disk" && res.GetDisk() != nil {
			persistentDiskRes = res

		} else if res.GetName() == "disk" {
			sandboxDiskRes = res
		}
	}

	suite.NotNil(persistentDiskRes)
	suite.Equal(persistentDiskRes.GetScalar().GetValue(), float64(3*numTasks*_disk))
	suite.Equal(persistentDiskRes.GetDisk().GetVolume().GetContainerPath(), testContainerPath)
	suite.Equal(persistentDiskRes.GetDisk().GetPersistence().GetId(), testVolumeID)

	suite.NotNil(sandboxDiskRes)
	suite.Equal(sandboxDiskRes.GetScalar().GetValue(), float64(numTasks*_disk))
}

// TestPopulateReservationVolumeInfo tests populateReservationInfo.
func (suite *BuilderTestSuite) TestPopulateReservationVolumeInfoNoVolume() {
	numTasks := 1
	resources := suite.getResources(numTasks)
	labels := &mesos.Labels{
		Labels: []*mesos.Label{
			{
				Key:   &_tmpLabelKey,
				Value: &_tmpLabelValue,
			},
		},
	}
	var err error
	resources, err = populateReservationVolumeInfo(resources, labels, nil)
	suite.Error(err)
}

func TestBuilderTestSuite(t *testing.T) {
	suite.Run(t, new(BuilderTestSuite))
}
