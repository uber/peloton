package fixture

import (
	"fmt"

	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api"
	"code.uber.internal/infra/peloton/util/randutil"
	"go.uber.org/thriftrw/ptr"
)

// AuroraJobKey returns a random JobKey.
func AuroraJobKey() *api.JobKey {
	return &api.JobKey{
		Role:        ptr.String(fmt.Sprintf("svc-%s", randutil.Text(6))),
		Environment: ptr.String(fmt.Sprintf("dep-%s", randutil.Text(6))),
		Name:        ptr.String(fmt.Sprintf("app-%s", randutil.Text(6))),
	}
}

// AuroraTaskConfig returns a random TaskConfig.
func AuroraTaskConfig() *api.TaskConfig {
	return &api.TaskConfig{
		Job: AuroraJobKey(),
	}
}

// AuroraJobUpdateRequest returns a random JobUpdateRequest.
func AuroraJobUpdateRequest() *api.JobUpdateRequest {
	return &api.JobUpdateRequest{
		TaskConfig: AuroraTaskConfig(),
	}
}
