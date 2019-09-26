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

package handler

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/apachemesos"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/volume"
	aurora "github.com/uber/peloton/.gen/thrift/aurora/api"
	"go.uber.org/yarpc/yarpcerrors"

	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/taskconfig"
	"github.com/uber/peloton/pkg/common/thermos"
	"github.com/uber/peloton/pkg/jobmgr/util/expansion"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.uber.org/thriftrw/ptr"
)

const (
	_thermosContainerName = "thermos-container"
	_jobEnvironment       = "us1.production"
	_auroraLabelPrefix    = "org.apache.aurora.metadata."

	MbInBytes = 1024 * 1024
)

// portSpecByName sorts a list of port spec by name
type portSpecByName []*pod.PortSpec

func (p portSpecByName) Len() int {
	return len(p)
}

func (p portSpecByName) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p portSpecByName) Less(i, j int) bool {
	return strings.Compare(p[i].GetName(), p[j].GetName()) < 0
}

// ConvertForThermosExecutor takes JobSpec as an input, generates and attaches
// thermos executor data if conversion could happen, and returns a mutated
// version of JobSpec.
func ConvertForThermosExecutor(
	jobSpec *stateless.JobSpec,
	thermosConfig config.ThermosExecutorConfig,
) (*stateless.JobSpec, error) {
	defaultSpec := jobSpec.GetDefaultSpec()
	convert, err := requiresThermosConvert(defaultSpec)
	if err != nil {
		return nil, err
	}
	if convert {
		newSpec, err := convertPodSpec(
			defaultSpec,
			defaultSpec,
			jobSpec,
			thermosConfig,
		)
		if err != nil {
			return nil, err
		}
		jobSpec.DefaultSpec = newSpec
	}

	for instanceID, instanceSpec := range jobSpec.GetInstanceSpec() {
		mergedSpec := taskconfig.MergePodSpec(defaultSpec, instanceSpec)
		convert, err := requiresThermosConvert(mergedSpec)
		if err != nil {
			return nil, err
		}
		if convert {
			newSpec, err := convertPodSpec(
				instanceSpec,
				mergedSpec,
				jobSpec,
				thermosConfig,
			)
			if err != nil {
				return nil, err
			}
			jobSpec.InstanceSpec[instanceID] = newSpec
		}
	}

	return jobSpec, nil
}

// requiresThermosConvert checks if the Peloton PodSpec requires thermos
// executor conversion. Throws an error if the PodSpec requires conversion,
// but fails validation.
func requiresThermosConvert(podSpec *pod.PodSpec) (bool, error) {
	if podSpec == nil {
		return false, nil
	}

	// Requires thermos data conversion, if MesosSpec inside PodSpec is
	// custom executor and executor data is empty.
	mesosSpec := podSpec.GetMesosSpec()
	if mesosSpec.GetExecutorSpec().GetType() != apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM ||
		len(mesosSpec.GetExecutorSpec().GetData()) > 0 {
		return false, nil
	}

	containers := podSpec.GetContainers()
	if len(containers) == 0 {
		return false, nil
	}

	// Container image must be defined for main container
	mainContainer := containers[0]
	if len(mainContainer.GetImage()) == 0 {
		return false, yarpcerrors.InvalidArgumentErrorf("container image must be defined")
	}

	// Check if name of the containers are defined, and does not have duplicates
	containerNames := map[string]struct{}{}
	for _, c := range append(
		podSpec.GetInitContainers(),
		podSpec.GetContainers()...,
	) {
		n := c.GetName()
		if len(n) == 0 {
			return false, yarpcerrors.InvalidArgumentErrorf("container does not have name specified")
		}
		if _, ok := containerNames[n]; ok {
			return false, yarpcerrors.InvalidArgumentErrorf("duplicate name found in container names")
		}
		containerNames[n] = struct{}{}
	}

	// Verify volumes and build volumes map
	volumes := map[string]*volume.VolumeSpec{}
	for _, v := range podSpec.GetVolumes() {
		if len(v.GetName()) == 0 {
			return false, yarpcerrors.InvalidArgumentErrorf("volume does not have name specified")
		}

		if _, ok := volumes[v.GetName()]; ok {
			return false, yarpcerrors.InvalidArgumentErrorf("duplicate volume name found in pod")
		}

		switch v.GetType() {
		case volume.VolumeSpec_VOLUME_TYPE_EMPTY_DIR:
			return false, yarpcerrors.InvalidArgumentErrorf("empty dir volume type not supported for volume: %s", v.GetName())
		case volume.VolumeSpec_VOLUME_TYPE_HOST_PATH:
			if len(v.GetHostPath().GetPath()) == 0 {
				return false, yarpcerrors.InvalidArgumentErrorf("path is empty for host_path volume")
			}
		case volume.VolumeSpec_VOLUME_TYPE_INVALID:
			return false, yarpcerrors.InvalidArgumentErrorf("invalid volume type for volume: %s", v.GetName())
		}

		volumes[v.GetName()] = v
	}

	// Verify all containers for volume mounts and environment variables
	envs := map[string]struct{}{}
	mounts := map[string]struct{}{}

	for _, c := range append(
		podSpec.GetInitContainers(),
		podSpec.GetContainers()...,
	) {
		// Verify volume mounts
		for _, m := range c.GetVolumeMounts() {
			if len(m.GetName()) == 0 {
				return false, yarpcerrors.InvalidArgumentErrorf("volume mount does not specify volume name")
			}

			if len(m.GetMountPath()) == 0 {
				return false, yarpcerrors.InvalidArgumentErrorf("volume mount does not specify mount path")
			}

			if _, ok := volumes[m.GetName()]; !ok {
				return false, yarpcerrors.InvalidArgumentErrorf("volume not defined: %s", m.GetName())
			}

			if _, ok := mounts[m.GetName()]; ok {
				return false, yarpcerrors.InvalidArgumentErrorf("duplicate volume mount not allowed")
			}

			mounts[m.GetName()] = struct{}{}
		}

		// Verify environment variables
		for _, e := range c.GetEnvironment() {
			if len(e.GetName()) == 0 {
				return false, yarpcerrors.InvalidArgumentErrorf("environment variable name not defined")
			}

			if _, ok := envs[e.GetName()]; ok {
				return false, yarpcerrors.InvalidArgumentErrorf("duplicate environment variable not allowed")
			}

			envs[e.GetName()] = struct{}{}
		}
	}

	return true, nil
}

// collectResources collects resources (including ports) from all containers
// in the PodSpec.
func collectResources(podSpec *pod.PodSpec) (*pod.ResourceSpec, []*pod.PortSpec) {
	// Collect maximum resource and ports allocated (key'ed by port name)
	// by initial containers.
	maxInitRes := &pod.ResourceSpec{}
	initPorts := make(map[string]*pod.PortSpec)
	for _, initContainer := range podSpec.GetInitContainers() {
		res := initContainer.GetResource()
		if res.GetCpuLimit() > maxInitRes.GetCpuLimit() {
			maxInitRes.CpuLimit = res.CpuLimit
		}
		if res.GetMemLimitMb() > maxInitRes.GetMemLimitMb() {
			maxInitRes.MemLimitMb = res.MemLimitMb
		}
		if res.GetDiskLimitMb() > maxInitRes.GetDiskLimitMb() {
			maxInitRes.DiskLimitMb = res.DiskLimitMb
		}
		if res.GetFdLimit() > maxInitRes.GetFdLimit() {
			maxInitRes.FdLimit = res.FdLimit
		}
		if res.GetGpuLimit() > maxInitRes.GetGpuLimit() {
			maxInitRes.GpuLimit = res.GpuLimit
		}

		for _, port := range initContainer.GetPorts() {
			if _, ok := initPorts[port.GetName()]; !ok {
				initPorts[port.GetName()] = port
			}
		}
	}

	// Collect sum of resources and ports allocated (key'ed by port name)
	// by containers
	sumRes := &pod.ResourceSpec{}
	ports := make(map[string]*pod.PortSpec)
	for _, container := range podSpec.GetContainers() {
		res := container.GetResource()
		sumRes.CpuLimit = sumRes.GetCpuLimit() + res.GetCpuLimit()
		sumRes.MemLimitMb = sumRes.GetMemLimitMb() + res.GetMemLimitMb()
		sumRes.DiskLimitMb = sumRes.GetDiskLimitMb() + res.GetDiskLimitMb()
		sumRes.FdLimit = sumRes.GetFdLimit() + res.GetFdLimit()
		sumRes.GpuLimit = sumRes.GetGpuLimit() + res.GetGpuLimit()

		for _, port := range container.GetPorts() {
			if _, ok := ports[port.GetName()]; !ok {
				ports[port.GetName()] = port
			}
		}
	}

	// Returned resource would be max of (maxInitRes, sumRes)
	// Returned ports would be merged list of (initPorts, ports)
	resultRes := &pod.ResourceSpec{
		CpuLimit:    math.Max(maxInitRes.GetCpuLimit(), sumRes.GetCpuLimit()),
		MemLimitMb:  math.Max(maxInitRes.GetMemLimitMb(), sumRes.GetMemLimitMb()),
		DiskLimitMb: math.Max(maxInitRes.GetDiskLimitMb(), sumRes.GetDiskLimitMb()),
		GpuLimit:    math.Max(maxInitRes.GetGpuLimit(), sumRes.GetGpuLimit()),
		// Using a function here since math.Max only supports float64
		FdLimit: func() uint32 {
			if maxInitRes.GetFdLimit() > sumRes.GetFdLimit() {
				return maxInitRes.GetFdLimit()
			}
			return sumRes.GetFdLimit()
		}(),
	}

	portsMap := make(map[string]*pod.PortSpec)
	for n, p := range initPorts {
		if _, ok := portsMap[n]; !ok {
			portsMap[n] = p
		}
	}
	for n, p := range ports {
		if _, ok := portsMap[n]; !ok {
			portsMap[n] = p
		}
	}
	resultPorts := make([]*pod.PortSpec, 0, len(portsMap))
	for _, p := range portsMap {
		resultPorts = append(resultPorts, p)
	}

	// Make sure the order of the ports are consistent to avoid
	// unnecessary job restarts.
	sort.Stable(portSpecByName(resultPorts))

	return resultRes, resultPorts
}

// createDockerInfo create mesos DockerInfo struct from ContainerSpec
func createDockerInfo(podSpec *pod.PodSpec) *mesos.ContainerInfo_DockerInfo {
	// TODO: Validate parameters are set in ContainerSpec
	// TODO: Right now we are overriding all the custom docker
	//  parameters passed in, thus we need to figure out some way to support
	//  thinsg like ulimit, cap-add, pids-limit etc.

	var params []*mesos.Parameter
	mainContainer := podSpec.GetContainers()[0]

	// Build volumes map
	volumes := map[string]*volume.VolumeSpec{}
	for _, v := range podSpec.GetVolumes() {
		volumes[v.GetName()] = v
	}

	for _, c := range append(
		podSpec.GetInitContainers(),
		podSpec.GetContainers()...,
	) {
		// Generate docker environment parameters
		for _, env := range c.GetEnvironment() {
			params = append(params, &mesos.Parameter{
				Key:   ptr.String("env"),
				Value: ptr.String(env.GetName() + "=" + env.GetValue()),
			})
		}

		// Generate docker volume parameters
		for _, mount := range c.GetVolumeMounts() {
			var param *mesos.Parameter
			v := volumes[mount.GetName()]
			switch v.GetType() {
			case volume.VolumeSpec_VOLUME_TYPE_HOST_PATH:
				value := v.GetHostPath().GetPath() + ":" + mount.GetMountPath() + ":"
				if mount.GetReadOnly() {
					value += "ro"
				} else {
					value += "rw"
				}
				param = &mesos.Parameter{
					Key:   ptr.String("volume"),
					Value: ptr.String(value),
				}
			}

			if param != nil {
				params = append(params, param)
			}
		}
	}

	return &mesos.ContainerInfo_DockerInfo{
		Image:      ptr.String(mainContainer.GetImage()),
		Parameters: params,
	}
}

// convert converts Peloton PodSpec to Aurora TaskConfig thrift structure.
func convert(
	jobSpec *stateless.JobSpec,
	podSpec *pod.PodSpec,
) (*aurora.TaskConfig, error) {
	collectedRes, collectedPorts := collectResources(podSpec)

	// tier
	tier := convertTier(jobSpec)

	// resources
	var resources []*aurora.Resource
	if collectedRes.GetCpuLimit() > 0 {
		resources = append(resources, &aurora.Resource{
			NumCpus: ptr.Float64(collectedRes.GetCpuLimit()),
		})
	}
	if collectedRes.GetMemLimitMb() > 0 {
		resources = append(resources, &aurora.Resource{
			RamMb: ptr.Int64(int64(collectedRes.GetMemLimitMb())),
		})
	}
	if collectedRes.GetDiskLimitMb() > 0 {
		resources = append(resources, &aurora.Resource{
			DiskMb: ptr.Int64(int64(collectedRes.GetDiskLimitMb())),
		})
	}
	if collectedRes.GetGpuLimit() > 0 {
		resources = append(resources, &aurora.Resource{
			NumGpus: ptr.Int64(int64(collectedRes.GetGpuLimit())),
		})
	}
	for _, port := range collectedPorts {
		resources = append(resources, &aurora.Resource{
			NamedPort: ptr.String(port.GetName()),
		})
	}

	// metadata
	metadata := make([]*aurora.Metadata, 0, len(podSpec.GetLabels()))
	for _, label := range podSpec.GetLabels() {
		key := label.GetKey()
		value := label.GetValue()
		// Aurora attaches "org.apache.aurora.metadata." prefix when
		// translating job metadata to mesos task label, reverting the
		// behavior here.
		if strings.HasPrefix(key, _auroraLabelPrefix) {
			key = strings.TrimPrefix(key, _auroraLabelPrefix)
		}
		metadata = append(metadata, &aurora.Metadata{
			Key:   ptr.String(key),
			Value: ptr.String(value),
		})
	}

	// container
	docker := createDockerInfo(podSpec)
	dockerParams := make([]*aurora.DockerParameter, 0, len(docker.GetParameters()))
	for _, param := range docker.GetParameters() {
		dockerParams = append(dockerParams, &aurora.DockerParameter{
			Name:  ptr.String(param.GetKey()),
			Value: ptr.String(param.GetValue()),
		})
	}
	container := &aurora.Container{
		Docker: &aurora.DockerContainer{
			Image:      ptr.String(docker.GetImage()),
			Parameters: dockerParams,
		},
	}

	// executor_data
	executorData, err := convertExecutorData(jobSpec, podSpec)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert pod spec to executor data")
	}

	executorDataStr, err := json.Marshal(executorData)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal executor data")
	}

	// task_config
	t := &aurora.TaskConfig{
		Job: &aurora.JobKey{
			Role:        ptr.String(jobSpec.GetName()),
			Environment: ptr.String(_jobEnvironment),
			Name:        ptr.String(jobSpec.GetName() + "." + jobSpec.GetName()),
		},
		IsService:  ptr.Bool(true),
		Priority:   ptr.Int32(int32(jobSpec.GetSla().GetPriority())),
		Production: ptr.Bool(false),
		Tier:       ptr.String(tier),
		Resources:  resources,
		Metadata:   metadata,
		Container:  container,
		ExecutorConfig: &aurora.ExecutorConfig{
			Name: ptr.String("AuroraExecutor"),
			Data: ptr.String(string(executorDataStr)),
		},
		// TODO: Fill MaxTaskFailures
	}

	if len(jobSpec.GetOwner()) > 0 {
		t.Owner = &aurora.Identity{User: ptr.String(jobSpec.GetOwner())}
	}

	return t, nil
}

// convertHealthCheckConfig generates HealthCheckConfig struct based on
// mainContainer's livenessCheck config.
func convertHealthCheckConfig(
	envs []*pod.Environment,
	livenessCheck *pod.HealthCheckSpec,
) (*HealthCheckConfig, error) {
	if !livenessCheck.GetEnabled() {
		return nil, nil
	}

	healthCheckConfig := NewHealthCheckConfig()

	switch livenessCheck.GetType() {
	case pod.HealthCheckSpec_HEALTH_CHECK_TYPE_COMMAND:
		healthCheckConfig.HealthChecker = NewHealthCheckerConfig()
		healthCheckConfig.HealthChecker.Shell = NewShellHealthChecker()
		healthCheckConfig.HealthChecker.Shell.ShellCommand = ptr.String(convertCmdline(envs, livenessCheck.GetCommand()))

	case pod.HealthCheckSpec_HEALTH_CHECK_TYPE_HTTP:
		var schema string
		if len(livenessCheck.GetHttpGet().GetScheme()) > 0 {
			schema = livenessCheck.GetHttpGet().GetScheme()
		} else {
			schema = "http"
		}
		endpoint := schema + "://127.0.0.1"
		if livenessCheck.GetHttpGet().GetPort() > 0 {
			endpoint += ":" + strconv.Itoa(int(livenessCheck.GetHttpGet().GetPort()))
		}
		endpoint += livenessCheck.GetHttpGet().GetPath()
		healthCheckConfig.HealthChecker = NewHealthCheckerConfig()
		healthCheckConfig.HealthChecker.Http = NewHttpHealthChecker()
		healthCheckConfig.HealthChecker.Http.Endpoint = ptr.String(endpoint)

	default:
		return nil, yarpcerrors.InvalidArgumentErrorf("unsupported liveness check type: %s", livenessCheck.GetType())
	}

	if livenessCheck.GetInitialIntervalSecs() > 0 {
		healthCheckConfig.InitialIntervalSecs = ptr.Float64(float64(livenessCheck.GetInitialIntervalSecs()))
	}

	if livenessCheck.GetIntervalSecs() > 0 {
		healthCheckConfig.IntervalSecs = ptr.Float64(float64(livenessCheck.GetIntervalSecs()))
	}

	if livenessCheck.GetMaxConsecutiveFailures() > 0 {
		healthCheckConfig.MaxConsecutiveFailures = ptr.Int32(int32(livenessCheck.GetMaxConsecutiveFailures()))
	}

	if livenessCheck.GetSuccessThreshold() > 0 {
		healthCheckConfig.MinConsecutiveSuccesses = ptr.Int32(int32(livenessCheck.GetSuccessThreshold()))
	}

	if livenessCheck.GetTimeoutSecs() > 0 {
		healthCheckConfig.TimeoutSecs = ptr.Float64(float64(livenessCheck.GetTimeoutSecs()))
	}

	return healthCheckConfig, nil
}

// convertTask generates Task struct based on JobSpec, PodSpec and ResourceSpec.
func convertTask(jobSpec *stateless.JobSpec, podSpec *pod.PodSpec, res *pod.ResourceSpec) *Task {
	task := NewTask()
	task.Name = ptr.String(jobSpec.GetName())

	if podSpec.GetKillGracePeriodSeconds() > 0 {
		task.FinalizationWait = ptr.Int32(int32(podSpec.GetKillGracePeriodSeconds()))
	}

	task.Resources = NewResources()

	if res.GetCpuLimit() > 0 {
		task.Resources.Cpu = ptr.Float64(res.GetCpuLimit())
	}

	if res.GetMemLimitMb() > 0 {
		task.Resources.RamBytes = ptr.Int64(int64(res.GetMemLimitMb() * MbInBytes))
	}

	if res.GetDiskLimitMb() > 0 {
		task.Resources.DiskBytes = ptr.Int64(int64(res.GetDiskLimitMb() * MbInBytes))
	}

	if res.GetGpuLimit() > 0 {
		task.Resources.Gpu = ptr.Int32(int32(res.GetGpuLimit()))
	}

	// Start init containers in order defined, and all init containers before
	// regular containers
	for _, c := range podSpec.GetContainers() {
		constraint := NewConstraint()
		for _, ic := range podSpec.GetInitContainers() {
			constraint.Order = append(constraint.Order, ptr.String(ic.GetName()))
		}
		constraint.Order = append(constraint.Order, ptr.String(c.GetName()))
		task.Constraints = append(task.Constraints, constraint)
	}

	// Convert ContainerSpecs to Processes
	for _, c := range append(
		podSpec.GetInitContainers(),
		podSpec.GetContainers()...,
	) {
		process := NewProcess()
		process.Name = ptr.String(c.GetName())
		process.Cmdline = ptr.String(convertCmdline(c.GetEnvironment(), c.GetEntrypoint()))
		task.Processes = append(task.Processes, process)
	}

	return task
}

// convertExecutorData generates ExecutorData struct based on Peloton PodSpec.
func convertExecutorData(
	jobSpec *stateless.JobSpec,
	podSpec *pod.PodSpec,
) (*ExecutorData, error) {
	mainContainer := podSpec.GetContainers()[0]
	collectedRes, _ := collectResources(podSpec)

	// health_check_config
	healthCheckConfig, err := convertHealthCheckConfig(
		mainContainer.GetEnvironment(),
		mainContainer.GetLivenessCheck(),
	)
	if err != nil {
		return nil, err
	}

	// task
	task := convertTask(jobSpec, podSpec, collectedRes)

	executorData := NewExecutorData()
	executorData.Role = ptr.String(jobSpec.GetName())
	executorData.Environment = ptr.String(_jobEnvironment)
	executorData.Name = ptr.String(jobSpec.GetName() + "." + jobSpec.GetName())
	executorData.Priority = ptr.Int32(int32(jobSpec.GetSla().GetPriority()))
	executorData.Production = ptr.Bool(false)
	executorData.Tier = ptr.String(convertTier(jobSpec))
	executorData.CronCollisionPolicy = ptr.String("KILL_EXISTING")
	executorData.MaxTaskFailures = ptr.Int32(1)
	executorData.EnableHooks = ptr.Bool(false)
	executorData.HealthCheckConfig = healthCheckConfig
	executorData.Task = task
	// TODO: Fill Cluster

	return executorData, nil
}

// convertTier returns tier string based on input Peloton JobSpec.
func convertTier(jobSpec *stateless.JobSpec) string {
	if jobSpec.GetSla().GetPreemptible() && jobSpec.GetSla().GetRevocable() {
		return "revocable"
	}
	return "preemptible"
}

// convertCmdline returns the command line string based on input Peloton
// CommandSpec. It replaces variables inside command arguments with actual
// values which defined in container environments, and wrap it in a single
// quote.
func convertCmdline(
	envs []*pod.Environment,
	command *pod.CommandSpec,
) string {
	envMap := map[string]string{}
	for _, env := range envs {
		envMap[env.GetName()] = env.GetValue()
	}

	mapping := expansion.MappingFuncFor(envMap)

	var cmd []string
	for _, arg := range append(
		[]string{command.GetValue()},
		command.GetArguments()...,
	) {
		// Each cmdline arg is generated in following steps:
		//   1. Do kubernetes style environment variable expansion, i.e.
		//      $(ENV) -> env-value, $$(ENV) -> $(ENV)
		//   2. Escape single quote characters by ending currently quoted
		//      string append single quote, then start a newly quoted string,
		//      i.e. ' -> '"'"'
		//   3. Wrap the whole argument in single quote
		cmd = append(cmd, "'"+strings.Replace(expansion.Expand(arg, mapping), "'", `'"'"'`, -1)+"'")
	}

	return strings.Join(cmd, " ")
}

// convertToData converts Peloton PodSpec to thermos executor data
// encoded in thrift binary protocol.
func convertToData(
	jobSpec *stateless.JobSpec,
	podSpec *pod.PodSpec,
) ([]byte, error) {
	taskConfig, err := convert(jobSpec, podSpec)
	if err != nil {
		return nil, err
	}

	taskConfigBytes, err := thermos.EncodeTaskConfig(taskConfig)
	if err != nil {
		return nil, err
	}

	return taskConfigBytes, nil
}

// mutatePodSpec mutates Peloton PodSpec to be usable by v1alpha API handler
// along with generated thermos executor data.
func mutatePodSpec(
	spec *pod.PodSpec,
	executorData []byte,
	thermosConfig config.ThermosExecutorConfig,
) error {
	collectedRes, collectedPorts := collectResources(spec)

	// Generate DockerInfo, CommandInfo and ExecutorInfo in Mesos v1 struct,
	// and convert them to the new fields.
	dockerInfo := createDockerInfo(spec)
	var dockerParameters []*apachemesos.PodSpec_DockerParameter
	for _, p := range dockerInfo.GetParameters() {
		dockerParameters = append(dockerParameters, &apachemesos.PodSpec_DockerParameter{
			Key:   p.GetKey(),
			Value: p.GetValue(),
		})
	}

	commandInfo := thermosConfig.NewThermosCommandInfo()
	var uris []*apachemesos.PodSpec_URI
	for _, u := range commandInfo.GetUris() {
		uris = append(uris, &apachemesos.PodSpec_URI{
			Value:      u.GetValue(),
			Executable: u.GetExecutable(),
		})
	}

	executorInfo := thermosConfig.NewThermosExecutorInfo(executorData)
	var executorType apachemesos.PodSpec_ExecutorSpec_ExecutorType
	executorResources := &apachemesos.PodSpec_ExecutorSpec_Resources{}
	switch executorInfo.GetType() {
	case mesos.ExecutorInfo_DEFAULT:
		executorType = apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_DEFAULT
	case mesos.ExecutorInfo_CUSTOM:
		executorType = apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM
	default:
		executorType = apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_INVALID
	}
	for _, r := range executorInfo.GetResources() {
		if r.GetType() == mesos.Value_SCALAR {
			if r.GetName() == "cpus" {
				executorResources.Cpu = r.GetScalar().GetValue()
			}
			if r.GetName() == "mem" {
				executorResources.MemMb = r.GetScalar().GetValue()
			}
		}
	}

	// Attach fields to PodSpec
	if spec.MesosSpec == nil {
		spec.MesosSpec = &apachemesos.PodSpec{}
	}
	spec.MesosSpec.Type = apachemesos.PodSpec_CONTAINER_TYPE_DOCKER
	spec.MesosSpec.DockerParameters = dockerParameters
	spec.MesosSpec.Uris = uris
	spec.MesosSpec.Shell = commandInfo.GetShell()
	spec.MesosSpec.ExecutorSpec = &apachemesos.PodSpec_ExecutorSpec{
		Type:       executorType,
		ExecutorId: executorInfo.GetExecutorId().GetValue(),
		Data:       executorInfo.GetData(),
		Resources:  executorResources,
	}

	mainContainer := spec.GetContainers()[0]
	mainContainer.Name = _thermosContainerName
	mainContainer.Image = dockerInfo.GetImage()
	mainContainer.Entrypoint = &pod.CommandSpec{
		Value: commandInfo.GetValue(),
	}
	mainContainer.Resource = collectedRes
	mainContainer.Ports = collectedPorts

	// Health check would be performed by Thermos Executor
	mainContainer.LivenessCheck = nil
	mainContainer.ReadinessCheck = nil

	// Clear out deprecated fields to avoid conflict
	mainContainer.Command = nil
	mainContainer.Container = nil
	mainContainer.Executor = nil

	// Environment and Volume has been translated to DockerParameters,
	// clear to avoid conflict.
	mainContainer.Environment = nil
	mainContainer.VolumeMounts = nil
	spec.Volumes = nil

	// Clear out init containers, since we expect those have already been
	// converted to processes in executor data
	spec.InitContainers = nil

	// Attach mutated container spec
	spec.Containers = []*pod.ContainerSpec{mainContainer}

	return nil
}

// convertPodSpec creates a copy of srcPodSpec which gets passed in, uses
// fullPodSpec (created by merging default and instance spec) to generate
// executor data and mutates the copy of srcPodSpec with thermos executor
// attached (along with a couple of other fields changed).
func convertPodSpec(
	srcPodSpec *pod.PodSpec,
	fullPodSpec *pod.PodSpec,
	jobSpec *stateless.JobSpec,
	thermosConfig config.ThermosExecutorConfig,
) (*pod.PodSpec, error) {
	newSpec := proto.Clone(srcPodSpec).(*pod.PodSpec)
	data, err := convertToData(jobSpec, fullPodSpec)
	if err != nil {
		return nil, err
	}
	err = mutatePodSpec(newSpec, data, thermosConfig)
	if err != nil {
		return nil, err
	}
	return newSpec, nil
}
