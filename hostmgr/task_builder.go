package hostmgr

import (
	"errors"
	"sort"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/util"

	mesos "mesos/v1"
	tc "peloton/api/task/config"
)

// taskBuilder helps to build launchable Mesos TaskInfo from offers and peloton task configs.
type taskBuilder struct {
	// A map from role -> scalar resources.
	scalars map[string]scalar.Resources

	// A map from role -> available ports
	portSets map[string]map[uint32]bool
}

// newTaskBuilder creates a new instance of taskBuilder, which caller can use to
// build Mesos tasks from the cached resources.
func newTaskBuilder(resources []*mesos.Resource) *taskBuilder {
	scalars := make(map[string]scalar.Resources)
	portSets := make(map[string]map[uint32]bool)
	for _, rs := range resources {
		tmp := scalar.FromMesosResource(rs)
		role := rs.GetRole()
		prev := scalars[role]
		scalars[role] = *(prev.Add(&tmp))

		ports := extractPortSet(rs)
		if len(ports) > 0 {
			if _, ok := portSets[role]; !ok {
				portSets[role] = ports
			} else {
				portSets[role] = mergePortSets(portSets[role], ports)
			}
		}
	}

	// TODO: Look into whether we need to prefer reserved (non-* role)
	// or unreserved (* role).
	return &taskBuilder{
		scalars:  scalars,
		portSets: portSets,
	}
}

// Helper struct for port picking result.
type portPickResult struct {
	// select ports numbers by name. This is used to populated DiscoveryInfo.Ports
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
func (tb *taskBuilder) pickPorts(taskConfig *tc.TaskConfig) (
	*portPickResult, error) {
	result := &portPickResult{
		selectedPorts: make(map[string]uint32),
		portEnvs:      make(map[string]string),
		portResources: []*mesos.Resource{},
	}

	if len(taskConfig.Ports) == 0 {
		return result, nil
	}

	// Name of dynamic ports.
	var dynamicNames []string

	for _, portConfig := range taskConfig.GetPorts() {
		name := portConfig.GetName()
		if len(name) == 0 {
			return nil, errors.New("Empty port name in task")
		}
		value := portConfig.GetValue()
		if value == 0 { // dynamic port
			dynamicNames = append(dynamicNames, name)
		} else {
			result.selectedPorts[name] = value
		}
	}

	// A map from role name to selected dynamic ports.
	rolePorts := make(map[string]map[uint32]bool)

	if len(dynamicNames) > 0 {
		index := 0
	Loop:
		for role, portSet := range tb.portSets {
			// Go lacks ways of random iterator for map, so this is
			// an approximate of random iterating of map.
			for port := range portSet {
				result.selectedPorts[dynamicNames[index]] = port
				delete(portSet, port)

				// Make sure rolePorts[role] is not nil.
				if _, ok2 := rolePorts[role]; !ok2 {
					rolePorts[role] = make(map[uint32]bool)
				}
				rolePorts[role][port] = true

				index++
				if index >= len(dynamicNames) {
					// We have selected enough dynamic ports.
					break Loop
				}
			}
		}

		if index < len(dynamicNames) {
			return nil, errors.New("No enough dynamic ports for task in Mesos offers")
		}
	} // end: if len(dynamicNames) > 0

	// Build Mesos resources objects, which will be used later to launch the task.
	for role, dynamicPorts := range rolePorts {
		ranges := createPortRanges(dynamicPorts)
		rs := util.NewMesosResourceBuilder().
			WithName("ports").
			WithRole(role).
			WithType(mesos.Value_RANGES).
			WithRanges(ranges).
			Build()
		result.portResources = append(result.portResources, rs)
	}

	// Populate extra environment variables, which will added to `CommandInfo`
	// to launch the task.
	for _, portConfig := range taskConfig.GetPorts() {
		if envName := portConfig.GetEnvName(); len(envName) == 0 {
			continue
		} else {
			p := int(result.selectedPorts[portConfig.GetName()])
			result.portEnvs[envName] = strconv.Itoa(p)
		}
	}

	return result, nil
}

// build is used to build a `mesos.TaskInfo` from cached resources. Caller can keep
// calling this function to build more tasks.
// This returns error if the current
// instance does not have enough resources leftover (scalar, port or
// even volume in future), or the taskConfig is not correct.
func (tb *taskBuilder) build(
	taskID *mesos.TaskID,
	taskConfig *tc.TaskConfig,
) (*mesos.TaskInfo, error) {

	// Validation of input.
	if taskConfig == nil {
		return nil, errors.New("TaskConfig cannot be nil")
	}

	if taskID == nil {
		return nil, errors.New("taskID cannot be nil")
	}

	taskResources := taskConfig.Resource
	if taskResources == nil {
		return nil, errors.New("TaskConfig.Resource cannot be nil")
	}

	tid, err := util.ParseTaskIDFromMesosTaskID(taskID.GetValue())
	if err != nil {
		return nil, err
	}

	jobID, _, err := util.ParseTaskID(tid)
	if err != nil {
		return nil, err
	}

	if taskConfig.GetCommand() == nil {
		return nil, errors.New("Command cannot be nil")
	}

	// A list of resources this task needs when launched to Mesos.
	launchResources, err := tb.extractScalarResources(taskConfig.GetResource())
	if err != nil {
		return nil, err
	}

	pick, err := tb.pickPorts(taskConfig)
	if err != nil {
		return nil, err
	}

	if len(pick.portResources) > 0 {
		launchResources = append(launchResources, pick.portResources...)
	}

	mesosTask := &mesos.TaskInfo{
		Name:      &jobID,
		TaskId:    taskID,
		Resources: launchResources,
	}

	tb.populateDiscoveryInfo(mesosTask, pick.selectedPorts, jobID)
	tb.populateCommandInfo(mesosTask, taskConfig.GetCommand(), pick.portEnvs)
	tb.populateContainerInfo(mesosTask, taskConfig.GetContainer())
	tb.populateLabels(mesosTask, taskConfig.GetLabels())

	return mesosTask, nil
}

// populateCommandInfo properly sets up the CommandInfo of a Mesos task
// and populates any optional environment variables in envMap.
func (tb *taskBuilder) populateCommandInfo(
	mesosTask *mesos.TaskInfo,
	command *mesos.CommandInfo,
	envMap map[string]string,
) {

	if command == nil {
		// Input validation happens before this, so this is a case that we want a nil
		// CommandInfo (aka custom executor case).
		return
	}

	// Make a deep copy of pass through fields to avoid changing input.
	mesosTask.Command = proto.Clone(command).(*mesos.CommandInfo)

	// Populate optional environment variables.
	if len(envMap) > 0 {
		// Make sure `Environment` field is initialized.
		if mesosTask.Command.GetEnvironment().GetVariables() == nil {
			mesosTask.Command.Environment = &mesos.Environment{
				Variables: []*mesos.Environment_Variable{},
			}
		}

		for name, value := range envMap {
			// Make a copy since taking address of key/value in map is dangerous.
			tmpName := name
			tmpValue := value
			env := &mesos.Environment_Variable{
				Name:  &tmpName,
				Value: &tmpValue,
			}
			mesosTask.Command.Environment.Variables =
				append(mesosTask.Command.Environment.Variables, env)
		}
	}
}

// populateContainerInfo properly sets up the `ContainerInfo` field of a mesos task.
func (tb *taskBuilder) populateContainerInfo(
	mesosTask *mesos.TaskInfo,
	container *mesos.ContainerInfo,
) {
	if container != nil {
		// Make a deep copy to avoid changing input.
		mesosTask.Container = proto.Clone(container).(*mesos.ContainerInfo)
	}
}

// populateLabels properly sets up the `Labels` field of a mesos task.
func (tb *taskBuilder) populateLabels(
	mesosTask *mesos.TaskInfo,
	labels *mesos.Labels,
) {
	if labels != nil {
		// Make a deep copy to avoid changing input.
		mesosTask.Labels = proto.Clone(labels).(*mesos.Labels)
	}
}

// populateDiscoveryInfo populates the `DiscoveryInfo` field of the task
// so service discovery integration can find which ports the task is using.
func (tb *taskBuilder) populateDiscoveryInfo(
	mesosTask *mesos.TaskInfo,
	selectedPorts map[string]uint32,
	jobID string,
) {
	if len(selectedPorts) > 0 {
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
			// TODO: 1. add Environment, Location, Version, and Labels;
			// 2. Determine how to find this in bridge.
		}
	}
}

// extractScalarResources takes necessary scalar resources from cached resources
// of this instance to construct a task, and returns error if not enough resources
// are left.
func (tb *taskBuilder) extractScalarResources(taskResources *tc.ResourceConfig) (
	[]*mesos.Resource, error) {

	if taskResources == nil {
		return nil, errors.New("Empty resources for task")
	}

	var launchResources []*mesos.Resource
	requiredScalar := scalar.FromResourceConfig(taskResources)
	for role, leftover := range tb.scalars {
		minimum := scalar.Minimum(leftover, requiredScalar)
		if minimum.Empty() {
			continue
		}
		rs := util.CreateMesosScalarResources(map[string]float64{
			"cpus": minimum.CPU,
			"mem":  minimum.Mem,
			"disk": minimum.Disk,
			"gpus": minimum.GPU,
		}, role)

		launchResources = append(launchResources, rs...)

		trySubtract := leftover.TrySubtract(&minimum)
		if trySubtract == nil {
			log.WithFields(log.Fields{
				"leftover": leftover,
				"minimum":  minimum,
			}).Warn("Incorrect resource amount in leftover!")
			return nil, errors.New("Incorrect resource amount in subtract!")
		}
		leftover = *trySubtract

		if leftover.Empty() {
			delete(tb.scalars, role)
		} else {
			tb.scalars[role] = leftover
		}

		trySubtract = requiredScalar.TrySubtract(&minimum)
		if trySubtract == nil {
			log.WithFields(log.Fields{
				"required": requiredScalar,
				"minimum":  minimum,
			}).Warn("Incorrect resource amount in required!")
			return nil, errors.New("Incorrect resource amount in subtract!")
		}
		requiredScalar = *trySubtract

		if requiredScalar.Empty() {
			break
		}
	}

	if !requiredScalar.Empty() {
		// TODO: return which resources are not sufficient.
		return nil, errors.New("Not enough resources left to run task")
	}
	return launchResources, nil
}

// extractPortSet is helper function to extract available port set
// from a Mesos resource.
func extractPortSet(resource *mesos.Resource) map[uint32]bool {
	res := make(map[uint32]bool)

	if resource.GetName() != "ports" {
		return res
	}

	for _, r := range resource.GetRanges().GetRange() {
		// Remember that end is inclusive
		for i := r.GetBegin(); i <= r.GetEnd(); i++ {
			res[uint32(i)] = true
		}
	}

	return res
}

func mergePortSets(m1, m2 map[uint32]bool) map[uint32]bool {
	m := make(map[uint32]bool)
	for i1, ok := range m1 {
		if ok {
			m[i1] = true
		}
	}
	for i2, ok := range m2 {
		if ok {
			m[i2] = true
		}
	}
	return m
}

// createPortRanges create Mesos Ranges type from given port set.
func createPortRanges(portSet map[uint32]bool) *mesos.Value_Ranges {
	var sorted []int
	for p, ok := range portSet {
		if ok {
			sorted = append(sorted, int(p))
		}
	}
	sort.Ints(sorted)

	res := mesos.Value_Ranges{
		Range: []*mesos.Value_Range{},
	}
	for _, p := range sorted {
		tmp := uint64(p)
		res.Range = append(res.Range, &mesos.Value_Range{Begin: &tmp, End: &tmp})
	}
	return &res
}
