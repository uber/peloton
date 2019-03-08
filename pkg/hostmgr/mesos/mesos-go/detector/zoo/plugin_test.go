package zoo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/detector"
)

// validate plugin registration for zk:// prefix is working
func TestDectorFactoryNew_ZkPrefix(t *testing.T) {
	assert := assert.New(t)
	m, err := detector.New("zk://127.0.0.1:5050/mesos")
	assert.NoError(err)
	assert.IsType(&MasterDetector{}, m)
	md := m.(*MasterDetector)
	t.Logf("canceling detector")
	md.Cancel()
}
