package zoo

import (
	"code.uber.internal/infra/peloton/mesos-go/detector"
)

func init() {
	detector.Register("zk://", detector.PluginFactory(func(spec string, options ...detector.Option) (detector.Master, error) {
		return NewMasterDetector(spec, options...)
	}))
}
