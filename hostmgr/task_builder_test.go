package hostmgr

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	mesos "mesos/v1"

	"peloton/api/task/config"

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

// helper function for getting a set of ports from ranges of integers.
func getPortSet(ranges ...uint32) map[uint32]bool {
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

// helper function for creating test task ids
func createTestTaskIDs(numTasks int) []*mesos.TaskID {
	var tids []*mesos.TaskID
	for i := 0; i < numTasks; i++ {
		tmp := fmt.Sprintf(taskIDFmt, i)
		tids = append(tids, &mesos.TaskID{Value: &tmp})
	}
	return tids
}

var (
	defaultResourceConfig = config.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
	}
)

// helper function to create a no-port task
func createTestTaskConfigs(numTasks int) []*config.TaskConfig {
	var configs []*config.TaskConfig
	tmpCmd := defaultCmd
	for i := 0; i < numTasks; i++ {
		configs = append(configs, &config.TaskConfig{
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
func testPortSetTransformation(
	t *testing.T, expected map[uint32]bool, rangeLen int) {
	ranges := createPortRanges(expected)
	assert.Equal(t, rangeLen, len(ranges.Range))

	rs := util.NewMesosResourceBuilder().
		WithName("ports").
		WithType(mesos.Value_RANGES).
		WithRanges(ranges).
		Build()
	portSet := extractPortSet(rs)
	assert.Equal(t, expected, portSet)
}

// This tests bidirection tranformation between set of available port and port
// ranges in Mesos resource.
func TestPortRanges(t *testing.T) {
	// Empty range.
	rs := util.NewMesosResourceBuilder().
		WithName("ports").
		WithType(mesos.Value_RANGES).
		Build()
	portSet := extractPortSet(rs)
	assert.Empty(t, portSet)

	// Unmatch resource name.
	rs = util.NewMesosResourceBuilder().
		WithName("foo").
		WithType(mesos.Value_RANGES).
		Build()
	portSet = extractPortSet(rs)
	assert.Empty(t, portSet)

	// Empty one
	testPortSetTransformation(t, getPortSet(), 0)
	// Single port
	testPortSetTransformation(t, getPortSet(1000, 1000), 1)
	// Single range
	testPortSetTransformation(t, getPortSet(1000, 1001), 2)
	// Single range
	testPortSetTransformation(t, getPortSet(1000, 1003), 4)
	// Multiple ranges.
	testPortSetTransformation(t, getPortSet(1000, 1001, 1003, 1004), 4)

	// Multiple single ports.
	testPortSetTransformation(
		t,
		getPortSet(1000, 1000, 2000, 2000, 3000, 3000, 4000, 4000),
		4)

	// Multiple longer ranges.
	testPortSetTransformation(
		t,
		getPortSet(1000, 2000, 3000, 4000),
		2002)
	// Merged ranges.
	testPortSetTransformation(
		t,
		getPortSet(1000, 2000, 2001, 3000),
		2001)
	// More ranges
	testPortSetTransformation(
		t,
		getPortSet(1000, 1001, 2000, 2001, 3000, 3001, 4000, 4000),
		7)
}

// This tests several copies of simple tasks without any port can be
// created as long as there are enough resources.
func TestNoPortTasks(t *testing.T) {
	resources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(40).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("mem").
			WithValue(40).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("disk").
			WithValue(40).
			Build(),
	}
	builder := newTaskBuilder(resources)
	assert.Equal(
		t,
		map[string]scalar.Resources{
			"*": {CPU: 40, Mem: 40, Disk: 40},
		},
		builder.scalars)
	assert.Empty(t, builder.portSets)
	numTasks := 4
	tids := createTestTaskIDs(numTasks)
	configs := createTestTaskConfigs(numTasks)

	for i := 0; i < numTasks; i++ {
		info, err := builder.build(tids[i], configs[i])
		assert.NoError(t, err)
		assert.Equal(t, tids[i], info.GetTaskId())
		sc := scalar.FromMesosResources(info.GetResources())
		assert.Equal(
			t,
			scalar.Resources{CPU: 10, Mem: 10, Disk: 10},
			sc)
	}

	// next build call will return an error due to insufficient resource.
	info, err := builder.build(tids[0], configs[0])
	assert.Nil(t, info)
	assert.EqualError(t, err, "Not enough resources left to run task")
}

// This tests several tasks requiring ports can be created.
func TestPortTasks(t *testing.T) {
	portSet := map[uint32]bool{
		1000: true,
		1002: true,
		1004: true,
		1006: true,
	}
	resources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(40).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("mem").
			WithValue(40).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("disk").
			WithValue(40).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("ports").
			WithType(mesos.Value_RANGES).
			WithRanges(createPortRanges(portSet)).
			Build(),
	}
	builder := newTaskBuilder(resources)
	assert.Equal(
		t,
		map[string]scalar.Resources{
			"*": {CPU: 40, Mem: 40, Disk: 40},
		},
		builder.scalars)
	assert.Equal(
		t,
		map[string]map[uint32]bool{"*": portSet},
		builder.portSets)
	numTasks := 2
	tid := createTestTaskIDs(1)[0]
	taskConfig := createTestTaskConfigs(1)[0]
	taskConfig.Ports = []*config.PortConfig{
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
		assert.NoError(t, err)
		assert.Equal(t, tid, info.GetTaskId())
		sc := scalar.FromMesosResources(info.GetResources())
		assert.Equal(
			t,
			scalar.Resources{CPU: 10, Mem: 10, Disk: 10},
			sc)
		discoveryInfo := info.GetDiscovery()
		assert.NotNil(t, discoveryInfo)
		assert.Equal(t, "testjob", discoveryInfo.GetName()) // job id
		mesosPorts := discoveryInfo.GetPorts().GetPorts()
		assert.Equal(t, 3, len(mesosPorts))

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

		assert.Equal(
			t,
			uint32(80),
			staticFound,
			"static port is not found in %v", mesosPorts)

		envVars := info.GetCommand().GetEnvironment().GetVariables()
		assert.Equal(t, 2, len(envVars))

		envMap := make(map[string]string)
		for _, envVar := range envVars {
			envMap[envVar.GetName()] = envVar.GetValue()
		}
		assert.Equal(t, 2, len(envMap))
		assert.Contains(t, envMap, "DYNAMIC_ENV_PORT")
		p, err := strconv.Atoi(envMap["DYNAMIC_ENV_PORT"])
		assert.NoError(t, err)
		assert.Contains(t, portSet, uint32(p))
		assert.Contains(t, envMap, "STATIC_PORT")
		assert.Equal(t, "80", envMap["STATIC_PORT"])

		assert.Equal(
			t,
			strconv.Itoa(int(portsInDiscovery["dynamic_env"])),
			envMap["DYNAMIC_ENV_PORT"])

		assert.Equal(t, taskConfig.Labels, info.Labels)
	}

	assert.Equal(t, portSet, discoveryPortSet)

	// next build call will return an error due to insufficient ports.
	info, err := builder.build(tid, taskConfig)
	assert.Nil(t, info)
	assert.EqualError(t, err, "No enough dynamic ports for task in Mesos offers")
}
