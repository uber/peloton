package task

import (
	"errors"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/util"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
)

var (
	_pelotonRole      = "peloton"
	_pelotonPrinciple = "peloton"
)

// Builder helps to build launchable Mesos TaskInfo from offers and
// peloton task configs.
type Builder struct {
	// A map from role -> scalar resources.
	scalars map[string]scalar.Resources

	// A map from role -> available ports
	portSets map[string]map[uint32]bool
}

// NewBuilder creates a new instance of Builder, which caller can use to
// build Mesos tasks from the cached resources.
func NewBuilder(resources []*mesos.Resource) *Builder {
	scalars := make(map[string]scalar.Resources)
	portSets := make(map[string]map[uint32]bool)
	for _, rs := range resources {
		tmp := scalar.FromMesosResource(rs)
		role := rs.GetRole()
		prev := scalars[role]
		scalars[role] = *(prev.Add(&tmp))

		ports := util.ExtractPortSet(rs)
		if len(ports) == 0 {
			continue
		}

		if _, ok := portSets[role]; !ok {
			portSets[role] = ports
		} else {
			portSets[role] = util.MergePortSets(portSets[role], ports)
		}
	}

	// TODO: Look into whether we need to prefer reserved (non-* role)
	// or unreserved (* role).
	return &Builder{
		scalars:  scalars,
		portSets: portSets,
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
	dynamicPorts := make(map[uint32]bool)
	for name, port := range selectedDynamicPorts {
		dynamicPorts[port] = true
		result.selectedPorts[name] = port
	}
	ranges := util.CreatePortRanges(dynamicPorts)
	rs := util.NewMesosResourceBuilder().
		WithName("ports").
		WithType(mesos.Value_RANGES).
		WithRanges(ranges).
		Build()
	result.portResources = append(result.portResources, rs)

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
// instance does not have enough resources leftover (scalar, port or
// even volume in future), or the taskConfig is not correct.
func (tb *Builder) Build(
	taskID *mesos.TaskID,
	taskConfig *task.TaskConfig,
	selectedDynamicPorts map[string]uint32,
	reservationLabels *mesos.Labels,
	volume *hostsvc.Volume,
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

	// lres is list of "launch" resources this task needs when launched.
	lres, err := tb.extractScalarResources(taskConfig.GetResource())
	if err != nil {
		return nil, err
	}

	pick, err := tb.pickPorts(taskConfig, selectedDynamicPorts)
	if err != nil {
		return nil, err
	}

	if len(pick.portResources) > 0 {
		lres = append(lres, pick.portResources...)
	}

	if reservationLabels != nil {
		populateReservationVolumeInfo(lres, reservationLabels, volume)
	}

	mesosTask := &mesos.TaskInfo{
		Name:      &jobID,
		TaskId:    taskID,
		Resources: lres,
	}

	tb.populateDiscoveryInfo(mesosTask, pick.selectedPorts, jobID)
	tb.populateCommandInfo(
		mesosTask,
		taskConfig.GetCommand(),
		pick.portEnvs,
	)
	tb.populateContainerInfo(mesosTask, taskConfig.GetContainer())
	tb.populateLabels(mesosTask, taskConfig.GetLabels())

	tb.populateHealthCheck(mesosTask, taskConfig.GetHealthCheck())

	return mesosTask, nil
}

// populateReservationVolumeInfo sets up the reservation and volume fields on
// mesos resources.
func populateReservationVolumeInfo(
	resources []*mesos.Resource,
	labels *mesos.Labels,
	volume *hostsvc.Volume) {

	if labels == nil || volume == nil {
		log.WithField("labels", labels).
			Error("volume is required to populate volume info")
		return
	}
	for _, res := range resources {
		if len(res.GetRole()) == 0 || res.GetRole() == "*" {
			// Setup the reservation if it is not reserved.
			res.Role = &_pelotonRole
			res.Reservation = &mesos.Resource_ReservationInfo{
				Principal: &_pelotonPrinciple,
				Labels:    proto.Clone(labels).(*mesos.Labels),
			}
		}
		if res.GetName() != "disk" || res.GetDisk() != nil {
			continue
		}
		// Setup the disk volume field.
		volumeID := volume.GetId().GetValue()
		containerPath := volume.GetContainerPath()
		volumeRWMode := mesos.Volume_RW
		res.Disk = &mesos.Resource_DiskInfo{
			Persistence: &mesos.Resource_DiskInfo_Persistence{
				Id:        &volumeID,
				Principal: &_pelotonPrinciple,
			},
			Volume: &mesos.Volume{
				ContainerPath: &containerPath,
				Mode:          &volumeRWMode,
			},
		}
	}
}

// populateCommandInfo properly sets up the CommandInfo of a Mesos task
// and populates any optional environment variables in envMap.
func (tb *Builder) populateCommandInfo(
	mesosTask *mesos.TaskInfo,
	command *mesos.CommandInfo,
	envMap map[string]string,
) {

	if command == nil {
		// Input validation happens before this, so this is a case that
		//  we want a nil CommandInfo (aka custom executor case).
		return
	}

	// Make a deep copy of pass through fields to avoid changing input.
	mesosTask.Command = proto.Clone(command).(*mesos.CommandInfo)

	if len(envMap) == 0 {
		return
	}

	// Populate optional environment variables.
	// Make sure `Environment` field is initialized.
	if mesosTask.Command.GetEnvironment().GetVariables() == nil {
		mesosTask.Command.Environment = &mesos.Environment{
			Variables: []*mesos.Environment_Variable{},
		}
	}

	for name, value := range envMap {
		// Make a copy since taking address of key/value in map
		// is dangerous.
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

// populateContainerInfo properly sets up the `ContainerInfo` field of a task.
func (tb *Builder) populateContainerInfo(
	mesosTask *mesos.TaskInfo,
	container *mesos.ContainerInfo,
) {
	if container != nil {
		// Make a deep copy to avoid changing input.
		mesosTask.Container =
			proto.Clone(container).(*mesos.ContainerInfo)
	}
}

// populateLabels properly sets up the `Labels` field of a mesos task.
func (tb *Builder) populateLabels(
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
	taskResources *task.ResourceConfig) ([]*mesos.Resource, error) {

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
			msg := "Incorrect resource amount in leftover!"
			log.WithFields(log.Fields{
				"leftover": leftover,
				"minimum":  minimum,
			}).Warn(msg)
			return nil, errors.New(msg)
		}
		leftover = *trySubtract

		if leftover.Empty() {
			delete(tb.scalars, role)
		} else {
			tb.scalars[role] = leftover
		}

		trySubtract = requiredScalar.TrySubtract(&minimum)
		if trySubtract == nil {
			msg := "Incorrect resource amount in required!"
			log.WithFields(log.Fields{
				"required": requiredScalar,
				"minimum":  minimum,
			}).Warn(msg)
			return nil, errors.New(msg)
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
