package thermos

import (
	"testing"

	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

// TestEncodeTaskConfig_Consistency make sure EncodeTaskConfig generated
// byte arrays are consistent across TaskConfigs whose some of the fields
// are different by order.
func TestEncodeTaskConfig_Consistency(t *testing.T) {
	t1 := &api.TaskConfig{
		Job: &api.JobKey{
			Role:        ptr.String("role"),
			Environment: ptr.String("environment"),
			Name:        ptr.String("name"),
		},
		Owner: &api.Identity{
			User: ptr.String("user"),
		},
		IsService:       ptr.Bool(true),
		Priority:        ptr.Int32(5),
		MaxTaskFailures: ptr.Int32(1),
		Production:      ptr.Bool(true),
		Tier:            ptr.String("revocable"),
		Resources: []*api.Resource{
			{NumCpus: ptr.Float64(1.5)},
			{RamMb: ptr.Int64(128)},
			{DiskMb: ptr.Int64(32)},
			{NamedPort: ptr.String("http")},
			{NamedPort: ptr.String("tchannel")},
			{NumGpus: ptr.Int64(2)},
		},
		Constraints: []*api.Constraint{
			{Name: ptr.String("mesos")},
			{Name: ptr.String("value")},
		},
		MesosFetcherUris: []*api.MesosFetcherURI{
			{Value: ptr.String("http://url1/")},
			{Value: ptr.String("http://url2/")},
		},
		ContactEmail: ptr.String("testuser@testdomain.com"),
		Metadata: []*api.Metadata{
			{
				Key:   ptr.String("test-key-1"),
				Value: ptr.String("test-value-1"),
			},
			{
				Key:   ptr.String("test-key-2"),
				Value: ptr.String("test-value-2"),
			},
		},
		ExecutorConfig: &api.ExecutorConfig{
			Data: ptr.String(`{"key1": "value1", "key2": "value2"}`),
		},
	}

	t2 := &api.TaskConfig{
		Job: &api.JobKey{
			Role:        ptr.String("role"),
			Environment: ptr.String("environment"),
			Name:        ptr.String("name"),
		},
		Owner: &api.Identity{
			User: ptr.String("user"),
		},
		IsService:       ptr.Bool(true),
		Priority:        ptr.Int32(5),
		MaxTaskFailures: ptr.Int32(1),
		Production:      ptr.Bool(true),
		Tier:            ptr.String("revocable"),
		Resources: []*api.Resource{
			{NamedPort: ptr.String("tchannel")},
			{NamedPort: ptr.String("http")},
			{RamMb: ptr.Int64(128)},
			{NumGpus: ptr.Int64(2)},
			{NumCpus: ptr.Float64(1.5)},
			{DiskMb: ptr.Int64(32)},
		},
		Constraints: []*api.Constraint{
			{Name: ptr.String("value")},
			{Name: ptr.String("mesos")},
		},
		MesosFetcherUris: []*api.MesosFetcherURI{
			{Value: ptr.String("http://url2/")},
			{Value: ptr.String("http://url1/")},
		},
		ContactEmail: ptr.String("testuser@testdomain.com"),
		Metadata: []*api.Metadata{
			{
				Key:   ptr.String("test-key-2"),
				Value: ptr.String("test-value-2"),
			},
			{
				Key:   ptr.String("test-key-1"),
				Value: ptr.String("test-value-1"),
			},
		},
		ExecutorConfig: &api.ExecutorConfig{
			Data: ptr.String(`{"key2": "value2", "key1": "value1"}`),
		},
	}

	b1, err := EncodeTaskConfig(t1)
	assert.NoError(t, err)

	b2, err := EncodeTaskConfig(t2)
	assert.NoError(t, err)

	assert.Equal(t, b1, b2)
}
