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

package lifecyclemgr

import (
	"bytes"
	"context"
	"strings"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	v0_hostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/models"
	aurora "github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/thriftrw/protocol"
	"go.uber.org/thriftrw/wire"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/time/rate"
)

// timeout for the orphan task kill call.
const _defaultKillTaskActionTimeout = 5 * time.Second

type v0LifecycleMgr struct {
	*lockState
	// v0 client for hostmgr task operations.
	hostManagerV0 v0_hostsvc.InternalHostServiceYARPCClient
	metrics       *Metrics
	retryPolicy   backoff.RetryPolicy
}

// Assignment information used to generate "AssignedTask"
type assignmentInfo struct {
	// Mesos task id
	taskID string
	// Mesos slave id that this task has been assigned to
	slaveID string
	// The name of the machine that this task has been assigned to
	slaveHost string
	// Ports reserved on the machine while this task is running
	assignedPorts map[string]int32
	// The instance ID assigned to this task
	instanceID int32
}

// newV0LifecycleMgr returns an instance of the v0 lifecycle manager.
func newV0LifecycleMgr(
	dispatcher *yarpc.Dispatcher,
	parent tally.Scope,
) *v0LifecycleMgr {
	return &v0LifecycleMgr{
		hostManagerV0: v0_hostsvc.NewInternalHostServiceYARPCClient(
			dispatcher.ClientConfig(
				common.PelotonHostManager),
		),
		lockState:   &lockState{state: 0},
		retryPolicy: backoff.NewRetryPolicy(3, 15*time.Second),
		metrics:     NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
	}
}

func (l *v0LifecycleMgr) kill(
	ctx context.Context,
	taskID string,
) error {
	req := &v0_hostsvc.KillTasksRequest{
		TaskIds: []*mesos.TaskID{{Value: &taskID}},
	}
	res, err := l.hostManagerV0.KillTasks(ctx, req)
	if err != nil {
		return err
	} else if e := res.GetError(); e != nil {
		switch {
		case e.KillFailure != nil:
			return yarpcerrors.InternalErrorf(e.KillFailure.Message)
		case e.InvalidTaskIDs != nil:
			return yarpcerrors.InternalErrorf(e.InvalidTaskIDs.Message)
		default:
			return yarpcerrors.InternalErrorf(e.String())
		}
	}
	return nil
}

func (l *v0LifecycleMgr) killAndReserve(
	ctx context.Context,
	taskID string,
	hostToReserve string,
) error {
	pelotonTaskID, err := util.ParseTaskIDFromMesosTaskID(taskID)
	if err != nil {
		return err
	}
	req := &v0_hostsvc.KillAndReserveTasksRequest{
		Entries: []*v0_hostsvc.KillAndReserveTasksRequest_Entry{
			{
				Id:            &peloton.TaskID{Value: pelotonTaskID},
				TaskId:        &mesos.TaskID{Value: &taskID},
				HostToReserve: hostToReserve,
			},
		},
	}
	res, err := l.hostManagerV0.KillAndReserveTasks(ctx, req)
	if err != nil {
		return err
	} else if e := res.GetError(); e != nil {
		switch {
		case e.KillFailure != nil:
			return yarpcerrors.InternalErrorf(e.KillFailure.Message)
		case e.InvalidTaskIDs != nil:
			return yarpcerrors.InternalErrorf(e.InvalidTaskIDs.Message)
		default:
			return yarpcerrors.InternalErrorf(e.String())
		}
	}
	return nil
}

// Launch launches the task using taskConfig. pod spec is ignored in this impl.
func (l *v0LifecycleMgr) Launch(
	ctx context.Context,
	leaseID string,
	hostname string,
	agentID string,
	tasks map[string]*LaunchableTaskInfo,
	rateLimiter *rate.Limiter,
) (err error) {
	defer func() {
		if err != nil && err != errLaunchInvalidOffer {
			if newErr := l.TerminateLease(
				ctx,
				hostname,
				agentID,
				leaseID,
			); newErr != nil {
				err = errors.Wrap(err, newErr.Error())
			}
		}
	}()

	if len(tasks) == 0 {
		return errEmptyTasks
	}
	// enforce rate limit
	if rateLimiter != nil && !rateLimiter.Allow() {
		l.metrics.LaunchRateLimit.Inc(1)
		return yarpcerrors.ResourceExhaustedErrorf("rate limit reached for kill")
	}

	log.WithField("tasks", tasks).Debug("Launching Tasks")
	callStart := time.Now()

	err = backoff.Retry(
		func() error {
			return l.launchBatchTasks(
				ctx,
				leaseID,
				hostname,
				agentID,
				tasks,
			)
		}, l.retryPolicy, l.isRetryableError)

	callDuration := time.Since(callStart)

	if err != nil {
		l.metrics.LaunchFail.Inc(int64(len(tasks)))
		return err
	}

	l.metrics.Launch.Inc(int64(len(tasks)))
	log.WithFields(log.Fields{
		"num_tasks": len(tasks),
		"hostname":  hostname,
		"duration":  callDuration.Seconds(),
	}).Debug("Launched tasks")
	l.metrics.LaunchDuration.Record(callDuration)
	return nil
}

func (l *v0LifecycleMgr) isRetryableError(err error) bool {
	switch err {
	case errLaunchInvalidOffer:
		return false
	}

	log.WithError(err).Warn("task launch need to be retried")
	l.metrics.LaunchRetry.Inc(1)
	return true
}

func (l *v0LifecycleMgr) launchBatchTasks(
	ctx context.Context,
	leaseID string,
	hostname string,
	agentID string,
	tasks map[string]*LaunchableTaskInfo,
) error {

	// convert LaunchableTaskInfo to v0 Hostsvc LaunchableTask
	var launchableTasks []*v0_hostsvc.LaunchableTask
	for _, launchableTaskInfo := range tasks {
		launchableTask := v0_hostsvc.LaunchableTask{
			TaskId: launchableTaskInfo.Runtime.GetMesosTaskId(),
			Config: launchableTaskInfo.Config,
			Ports:  launchableTaskInfo.Runtime.GetPorts(),
			Id: &peloton.TaskID{Value: util.CreatePelotonTaskID(
				launchableTaskInfo.GetJobId().GetValue(),
				launchableTaskInfo.GetInstanceId(),
			),
			},
		}
		err := populateExecutorData(&launchableTask, hostname, agentID)
		if err != nil {
			return err
		}
		mutateSystemLabels(&launchableTask, launchableTaskInfo.ConfigAddOn)
		launchableTasks = append(launchableTasks, &launchableTask)
	}

	ctx, cancel := context.WithTimeout(ctx, _defaultHostmgrAPITimeout)
	defer cancel()
	var request = &v0_hostsvc.LaunchTasksRequest{
		Hostname: hostname,
		Tasks:    launchableTasks,
		AgentId:  &mesos.AgentID{Value: &agentID},
		Id:       &peloton.HostOfferID{Value: leaseID},
	}

	log.WithField("request", request).Debug("LaunchTasks Called")

	response, err := l.hostManagerV0.LaunchTasks(ctx, request)
	if err != nil {
		return err
	}
	if response.GetError() != nil {
		log.WithFields(log.Fields{
			"response": response,
			"hostname": hostname,
		}).Error("hostmgr launch tasks got error resp")
		if response.GetError().GetInvalidOffers() != nil {
			return errLaunchInvalidOffer
		}
		return yarpcerrors.InternalErrorf(response.Error.String())
	}
	return nil
}

// Kill does one of two things:
// if a host is not provided, it tries to kill the task using taskID.
// if a host is provided, it kills the task and reserves the host.
func (l *v0LifecycleMgr) Kill(
	ctx context.Context,
	taskID string,
	hostToReserve string,
	rateLimiter *rate.Limiter,
) error {
	// check lock
	if l.lockState.hasKillLock() {
		return yarpcerrors.InternalErrorf("kill op is locked")
	}

	// enforce rate limit
	if rateLimiter != nil && !rateLimiter.Allow() {
		l.metrics.KillRateLimit.Inc(1)
		return yarpcerrors.ResourceExhaustedErrorf(
			"rate limit reached for kill")
	}

	newCtx := ctx
	_, ok := ctx.Deadline()
	if !ok {
		var cancelFunc context.CancelFunc
		newCtx, cancelFunc = context.WithTimeout(
			context.Background(), _defaultKillTaskActionTimeout)
		defer cancelFunc()
	}

	if len(hostToReserve) != 0 {
		return l.killAndReserve(newCtx, taskID, hostToReserve)
	}
	return l.kill(newCtx, taskID)
}

// ShutdownExecutor shutdown a executor given task ID and agent ID
func (l *v0LifecycleMgr) ShutdownExecutor(
	ctx context.Context,
	taskID string,
	agentID string,
	rateLimiter *rate.Limiter,
) error {
	// check lock
	if l.lockState.hasKillLock() {
		return yarpcerrors.InternalErrorf("shutdown executor op is locked")
	}

	// enforce rate limit
	if rateLimiter != nil && !rateLimiter.Allow() {
		return yarpcerrors.ResourceExhaustedErrorf(
			"rate limit reached for shutdown executor")
	}

	req := &v0_hostsvc.ShutdownExecutorsRequest{
		Executors: []*v0_hostsvc.ExecutorOnAgent{
			{
				ExecutorId: &mesos.ExecutorID{Value: &taskID},
				AgentId:    &mesos.AgentID{Value: &agentID},
			},
		},
	}

	res, err := l.hostManagerV0.ShutdownExecutors(ctx, req)

	if err != nil {
		return err
	} else if e := res.GetError(); e != nil {
		switch {
		case e.ShutdownFailure != nil:
			return yarpcerrors.InternalErrorf(e.ShutdownFailure.Message)
		case e.InvalidExecutors != nil:
			return yarpcerrors.InternalErrorf(e.InvalidExecutors.Message)
		default:
			return yarpcerrors.InternalErrorf(e.String())
		}
	}
	return nil
}

// TerminateLease returns the unused lease back to the hostmgr.
func (l *v0LifecycleMgr) TerminateLease(
	ctx context.Context,
	hostname string,
	agentID string,
	leaseID string,
) error {
	request := &v0_hostsvc.ReleaseHostOffersRequest{
		HostOffers: []*v0_hostsvc.HostOffer{{
			Hostname: hostname,
			AgentId:  &mesos.AgentID{Value: &agentID},
			Id:       &peloton.HostOfferID{Value: leaseID},
		}},
	}
	ctx, cancel := context.WithTimeout(ctx, _defaultHostmgrAPITimeout)
	defer cancel()
	_, err := l.hostManagerV0.ReleaseHostOffers(ctx, request)
	if err != nil {
		l.metrics.TerminateLeaseFail.Inc(1)
		return errors.Wrapf(err,
			"failed to terminate lease: %v host %v", leaseID, hostname,
		)
	}
	l.metrics.TerminateLease.Inc(1)
	return nil
}

// GetTasksOnDrainingHosts gets the taskIDs of the tasks on the
// hosts in DRAINING state
func (l *v0LifecycleMgr) GetTasksOnDrainingHosts(
	ctx context.Context,
	limit uint32,
	timeout uint32,
) ([]string, error) {
	request := &v0_hostsvc.GetTasksByHostStateRequest{
		HostState: pbhost.HostState_HOST_STATE_DRAINING,
		Limit:     limit,
		Timeout:   timeout,
	}

	response, err := l.hostManagerV0.GetTasksByHostState(ctx, request)
	if err != nil {
		l.metrics.GetTasksOnDrainingHostsFail.Inc(1)
		return nil, errors.Wrapf(err,
			"failed to get tasks on hosts in DRAINING state")
	}

	var taskIDs []string
	for _, t := range response.GetTaskIds() {
		taskIDs = append(taskIDs, t.GetValue())
	}

	l.metrics.GetTasksOnDrainingHosts.Inc(1)
	return taskIDs, nil
}

// populateExecutorData transforms executor data in TaskConfig to data
// usable by actual custom executor. Currently, it only supports aurora
// thermos executor, in which case, it will pack the existing executor
// data along with placement information to binary-serialized AssignedTask
// thrift struct.
func populateExecutorData(
	launchableTask *v0_hostsvc.LaunchableTask,
	hostname string,
	agentID string,
) error {
	executorData := launchableTask.GetConfig().GetExecutor().GetData()
	if launchableTask.GetConfig().GetExecutor().GetType() !=
		mesos.ExecutorInfo_CUSTOM || len(executorData) == 0 {
		return nil
	}

	taskID := launchableTask.GetTaskId().GetValue()
	_, instanceID, err := util.ParseJobAndInstanceID(taskID)
	if err != nil {
		return err
	}

	assignedPorts := make(map[string]int32)
	for name, num := range launchableTask.GetPorts() {
		assignedPorts[name] = int32(num)
	}
	assignment := assignmentInfo{
		taskID:        taskID,
		slaveID:       agentID,
		slaveHost:     hostname,
		assignedPorts: assignedPorts,
		instanceID:    int32(instanceID),
	}

	transformedData, err := generateAssignedTask(executorData, assignment)
	if err != nil {
		return err
	}
	launchableTask.GetConfig().GetExecutor().Data = transformedData

	return nil
}

// generateAssignedTask takes in binary form of "TaskConfig" thrift struct
// along with task assignment information, and generates "AssignedTask"
// thrift struct in binary form.
func generateAssignedTask(
	taskConfigData []byte,
	assignment assignmentInfo,
) ([]byte, error) {
	taskConfigWireValue, err := protocol.Binary.Decode(
		bytes.NewReader(taskConfigData),
		wire.TStruct,
	)
	if err != nil {
		return []byte{}, err
	}

	taskConfig := &aurora.TaskConfig{}
	err = taskConfig.FromWire(taskConfigWireValue)
	if err != nil {
		return []byte{}, err
	}

	assignedTask := &aurora.AssignedTask{}
	assignedTask.TaskId = &assignment.taskID
	assignedTask.SlaveId = &assignment.slaveID
	assignedTask.SlaveHost = &assignment.slaveHost
	assignedTask.Task = taskConfig
	assignedTask.AssignedPorts = assignment.assignedPorts
	assignedTask.InstanceId = &assignment.instanceID

	assignedTaskWireValue, err := assignedTask.ToWire()
	if err != nil {
		return []byte{}, err
	}
	var assignedTaskBuffer bytes.Buffer
	err = protocol.Binary.Encode(assignedTaskWireValue, &assignedTaskBuffer)
	if err != nil {
		return []byte{}, err
	}

	return assignedTaskBuffer.Bytes(), nil
}

// Strip off labels with prefix common.SystemLabelPrefix. This is a temporary
// fix to ensure job creates dont fail on clients adding the system labels
// TODO: remove this once all Peloton clients have been modified
// to not add these labels to jobs submitted through them.
func mutateSystemLabels(
	launchableTask *v0_hostsvc.LaunchableTask,
	addOn *models.ConfigAddOn,
) {
	var labels []*peloton.Label
	for _, label := range launchableTask.GetConfig().GetLabels() {
		if !strings.HasPrefix(label.GetKey(), common.SystemLabelPrefix+".") {
			labels = append(labels, label)
		}
	}
	launchableTask.Config.Labels = labels
	launchableTask.Config.Labels = append(
		launchableTask.Config.Labels, addOn.GetSystemLabels()...)
}
