package label

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
)

// Label defines a label.
type Label interface {
	Key() string
	Value() string
}

// Build builds l into a Peloton Label message.
func Build(l Label) *peloton.Label {
	return &peloton.Label{
		Key:   l.Key(),
		Value: l.Value(),
	}
}
