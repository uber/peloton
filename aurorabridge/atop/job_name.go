package atop

import (
	"fmt"

	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api"
)

// NewJobName creates a new job name.
func NewJobName(k *api.JobKey) string {
	// We use "/" as a delimiter because Aurora doesn't allow "/" in JobKey components,
	// and is also roughly consistent with how Aurora represents job paths.
	return fmt.Sprintf("%s/%s/%s", k.GetRole(), k.GetEnvironment(), k.GetName())
}
