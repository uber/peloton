package label

import (
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
)

// Label defines a label.
type Label interface {
	Key() string
	Value() string
}

// rawLabel implements Label interface, but allows setting key and value
// directly>
type rawLabel struct {
	key, value string
}

func (l *rawLabel) Key() string {
	return l.key
}

func (l *rawLabel) Value() string {
	return l.value
}

// Build builds l into a Peloton Label message.
func Build(l Label) *peloton.Label {
	return &peloton.Label{
		Key:   l.Key(),
		Value: l.Value(),
	}
}

// BuildMany builds a list of Labels into a list of Peloton Labels.
func BuildMany(labels []Label) []*peloton.Label {
	var pelotonLabels []*peloton.Label
	for _, label := range labels {
		pelotonLabels = append(pelotonLabels, Build(label))
	}
	return pelotonLabels
}
