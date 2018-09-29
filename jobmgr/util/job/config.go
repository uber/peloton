package job

import (
	"fmt"
	"os"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"

	"code.uber.internal/infra/peloton/common"
)

// ConstructSystemLabels constructs and returns system labels
func ConstructSystemLabels(jobConfig *job.JobConfig, respoolPath string) []*peloton.Label {
	return append(
		[]*peloton.Label{},
		&peloton.Label{
			Key: fmt.Sprintf(
				common.SystemLabelKeyTemplate,
				common.SystemLabelPrefix,
				common.SystemLabelCluster),
			Value: os.Getenv(common.ClusterEnvVar),
		},
		&peloton.Label{
			Key: fmt.Sprintf(
				common.SystemLabelKeyTemplate,
				common.SystemLabelPrefix,
				common.SystemLabelJobName),
			Value: jobConfig.GetName(),
		},
		&peloton.Label{
			Key: fmt.Sprintf(
				common.SystemLabelKeyTemplate,
				common.SystemLabelPrefix,
				common.SystemLabelJobOwner),
			Value: jobConfig.GetOwningTeam(),
		},
		&peloton.Label{
			Key: fmt.Sprintf(
				common.SystemLabelKeyTemplate,
				common.SystemLabelPrefix,
				common.SystemLabelJobType),
			Value: jobConfig.GetType().String(),
		},
		&peloton.Label{
			Key: fmt.Sprintf(
				common.SystemLabelKeyTemplate,
				common.SystemLabelPrefix,
				common.SystemLabelRespoolPath),
			Value: respoolPath,
		},
	)
}
