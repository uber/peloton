package updater

import (
	"fmt"
	"reflect"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/jobmgr/task"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

var _updateNotSupported = "updating %s not supported"

// JobDiff holds the difference between two job states
type JobDiff struct {
	// instanceID->runtime info
	InstancesToAdd map[uint32]*pb_task.RuntimeInfo
}

// CalculateJobDiff returns the difference between 2 jobs
func CalculateJobDiff(
	id *peloton.JobID,
	oldConfig *job.JobConfig,
	newConfig *job.JobConfig) (JobDiff, error) {
	var jobDiff JobDiff

	err := validateNewConfig(oldConfig, newConfig)
	if err != nil {
		return jobDiff, errors.Wrapf(err, "config validation failed")
	}
	instanceToAdd, err := getInstancesToAdd(id, oldConfig, newConfig)
	if err != nil {
		return jobDiff, err
	}

	jobDiff.InstancesToAdd = instanceToAdd
	return jobDiff, nil
}

// IsNoop checks whether this diff contains no work to be done
func (jd *JobDiff) IsNoop() bool {
	return len(jd.InstancesToAdd) == 0
}

// validateNewConfig validates the changes in the new config
func validateNewConfig(oldConfig *job.JobConfig,
	newConfig *job.JobConfig) error {
	errs := new(multierror.Error)
	if oldConfig.Name != newConfig.Name {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "Name"))
	}

	if !reflect.DeepEqual(oldConfig.Labels, newConfig.Labels) {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "Labels"))
	}

	if oldConfig.OwningTeam != newConfig.OwningTeam {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "OwningTeam"))
	}

	if oldConfig.RespoolID.GetValue() != newConfig.RespoolID.GetValue() {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "RespoolID"))
	}

	if oldConfig.Type != newConfig.Type {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "Type"))
	}

	if !reflect.DeepEqual(oldConfig.LdapGroups, newConfig.LdapGroups) {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "LdapGroups"))
	}
	if !reflect.DeepEqual(oldConfig.DefaultConfig, newConfig.DefaultConfig) {
		errs = multierror.Append(errs,
			fmt.Errorf(_updateNotSupported, "DefaultConfig"))
	}
	return errs.ErrorOrNil()
}

func getInstancesToAdd(
	jobID *peloton.JobID,
	oldConfig *job.JobConfig,
	newConfig *job.JobConfig) (map[uint32]*pb_task.RuntimeInfo, error) {

	instancesToAdd := make(map[uint32]*pb_task.RuntimeInfo)
	if oldConfig.InstanceCount == newConfig.InstanceCount {
		return instancesToAdd, nil
	}

	if newConfig.InstanceCount < oldConfig.InstanceCount {
		return instancesToAdd, errors.New("new instance count can't be less")
	}

	for i := oldConfig.InstanceCount; i < newConfig.InstanceCount; i++ {
		instancesToAdd[i] = task.CreateInitializingTask(jobID, i, newConfig)
	}
	return instancesToAdd, nil
}
