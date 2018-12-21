package job

import (
	"fmt"
	"strconv"
	"strings"

	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"

	"go.uber.org/yarpc/yarpcerrors"
)

// GetJobEntityVersion builds the job entity version from its components
func GetJobEntityVersion(
	configVersion uint64,
	desiredStateVersion uint64,
	workflowVersion uint64,
) *v1alphapeloton.EntityVersion {
	return &v1alphapeloton.EntityVersion{
		Value: fmt.Sprintf("%d-%d-%d", configVersion, desiredStateVersion, workflowVersion),
	}
}

// ParseJobEntityVersion parses job entity version into components
func ParseJobEntityVersion(
	entityVersion *v1alphapeloton.EntityVersion,
) (configVersion uint64, desiredStateVersion uint64, workflowVersion uint64, err error) {
	results := strings.Split(entityVersion.GetValue(), "-")
	if len(results) != 3 {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format")
		return
	}

	configVersion, err = strconv.ParseUint(results[0], 10, 64)
	if err != nil {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format: %s", err.Error())
	}

	desiredStateVersion, err = strconv.ParseUint(results[1], 10, 64)
	if err != nil {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format: %s", err.Error())
	}

	workflowVersion, err = strconv.ParseUint(results[2], 10, 64)
	if err != nil {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format: %s", err.Error())
	}
	return
}

// GetPodEntityVersion builds the pod entity version from its components
func GetPodEntityVersion(configVersion uint64) *v1alphapeloton.EntityVersion {
	return &v1alphapeloton.EntityVersion{
		Value: fmt.Sprintf("%d", configVersion),
	}
}

// ParsePodEntityVersion parses pod entity version into components
func ParsePodEntityVersion(
	entityVersion *v1alphapeloton.EntityVersion,
) (configVersion uint64, err error) {
	results := strings.Split(entityVersion.GetValue(), "-")
	if len(results) != 1 {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format")
		return
	}

	configVersion, err = strconv.ParseUint(results[0], 10, 64)
	if err != nil {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format: %s", err.Error())
	}

	return
}
