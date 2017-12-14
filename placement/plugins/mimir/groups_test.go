package mimir

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/placement/testutil"
)

func TestGroupMapper_Convert(t *testing.T) {
	offer := testutil.SetupHost().Offer()
	group := OfferToGroup(offer)
	assert.Equal(t, "hostname", group.Name)
	assert.Equal(t, 4800.0, group.Metrics.Get(metrics.CPUTotal))
	assert.Equal(t, 128.0*metrics.GiB, group.Metrics.Get(metrics.MemoryTotal))
	assert.Equal(t, 6.0*metrics.TiB, group.Metrics.Get(metrics.DiskTotal))
	assert.Equal(t, 12800.0, group.Metrics.Get(metrics.GPUTotal))
	assert.Equal(t, 10.0, group.Metrics.Get(metrics.PortsTotal))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "text")))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "1")))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "[31000-31009]")))
}
