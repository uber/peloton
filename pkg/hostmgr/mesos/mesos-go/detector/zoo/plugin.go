package zoo

import (
	"github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/detector"
)

func init() {
	detector.Register("zk://", detector.PluginFactory(func(spec string, options ...detector.Option) (detector.Master, error) {
		return NewMasterDetector(spec, options...)
	}))
}
