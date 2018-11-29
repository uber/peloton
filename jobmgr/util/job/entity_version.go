package job

import (
	"fmt"
	"strconv"
	"strings"

	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"

	"go.uber.org/yarpc/yarpcerrors"
)

// GetEntityVersion builds the entity version from its components
func GetEntityVersion(configVersion uint64) *v1alphapeloton.EntityVersion {
	return &v1alphapeloton.EntityVersion{
		Value: fmt.Sprintf("%d", configVersion),
	}
}

// ParseEntityVersion parses entity version into components
func ParseEntityVersion(entityVersion *v1alphapeloton.EntityVersion) (configVersion uint64, err error) {
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
