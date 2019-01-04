package mimir

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/peloton/mimir-lib/model/labels"
	"github.com/uber/peloton/mimir-lib/model/metrics"
	"github.com/uber/peloton/placement/testutil"
)

func TestGroupMapper_Convert(t *testing.T) {
	offer := testutil.SetupHostOffers().GetOffer()
	group := OfferToGroup(offer)
	assert.Equal(t, "hostname", group.Name)
	assert.Equal(t, 4800.0, group.Metrics.Get(CPUAvailable))
	assert.Equal(t, 128.0*metrics.GiB, group.Metrics.Get(MemoryAvailable))
	assert.Equal(t, 6.0*metrics.TiB, group.Metrics.Get(DiskAvailable))
	assert.Equal(t, 12800.0, group.Metrics.Get(GPUAvailable))
	assert.Equal(t, 10.0, group.Metrics.Get(PortsAvailable))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "text")))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "1")))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "[31000-31009]")))
}
