package fixture

import (
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/util/randutil"
)

// PelotonEntityVersion returns a random EntityVersion.
func PelotonEntityVersion() *peloton.EntityVersion {
	return &peloton.EntityVersion{
		Value: fmt.Sprintf("version-%s", randutil.Text(8)),
	}
}

// PelotonJobID returns a random JobID.
func PelotonJobID() *peloton.JobID {
	return &peloton.JobID{
		Value: fmt.Sprintf("job-%s", randutil.Text(8)),
	}
}

// PelotonResourcePoolID returns a random ResourcePoolID.
func PelotonResourcePoolID() *peloton.ResourcePoolID {
	return &peloton.ResourcePoolID{
		Value: fmt.Sprintf("respool-%s", randutil.Text(8)),
	}
}
