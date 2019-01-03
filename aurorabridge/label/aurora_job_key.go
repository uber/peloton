package label

import (
	"fmt"

	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api"
)

const _auroraJobKeyKey = "aurora_job_key"

// AuroraJobKey is a label for the original Aurora JobKey which was mapped into
// a job.
type AuroraJobKey struct {
	role, environment, name string
}

// NewAuroraJobKey creates a new AuroraJobKey label.
func NewAuroraJobKey(k *api.JobKey) *AuroraJobKey {
	return &AuroraJobKey{
		role:        k.GetRole(),
		environment: k.GetEnvironment(),
		name:        k.GetName(),
	}
}

// Key returns the label key.
func (i *AuroraJobKey) Key() string {
	return _auroraJobKeyKey
}

// Value returns the label value.
func (i *AuroraJobKey) Value() string {
	return fmt.Sprintf("%s/%s/%s", i.role, i.environment, i.name)
}
