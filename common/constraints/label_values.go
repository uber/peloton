package constraints

import (
	"strconv"

	log "github.com/Sirupsen/logrus"

	mesos "mesos/v1"
)

const (
	// HostNameKey is the special label key for hostname.
	HostNameKey = "hostname"

	_precision = 6
	_bitsize   = 64
)

// LabelValues tracks how many times a value presents for a given label key.
// First level key is label key, second level key is label value.
// This is the subject of constraint evaluation process.
type LabelValues map[string]map[string]uint32

// GetHostLabelValues returns label counts for a host and its attributes,
// which can be used to evaluate a constraint.
// NOTE: `hostname` is added unconditionally, to make sure hostname based
// constraints can be done regardless of attribute configuration.
func GetHostLabelValues(
	hostname string,
	attributes []*mesos.Attribute) LabelValues {

	result := make(map[string]map[string]uint32)
	result[HostNameKey] = map[string]uint32{hostname: 1}
OUTER:
	for _, attr := range attributes {
		key := attr.GetName()
		values := []string{}
		switch attr.GetType() {
		case mesos.Value_TEXT:
			values = append(values, attr.GetText().GetValue())
		case mesos.Value_SCALAR:
			value := strconv.FormatFloat(
				attr.GetScalar().GetValue(),
				'f',
				_precision,
				_bitsize)
			values = append(values, value)
		case mesos.Value_SET:
			for _, value := range attr.GetSet().GetItem() {
				values = append(values, value)
			}
		default:
			// TODO: Add support for range attributes.
			log.WithFields(log.Fields{
				"key":  key,
				"type": attr.GetType(),
			}).Warn("Attribute type is not supported yet")
			continue OUTER
		}
		if _, ok := result[key]; !ok {
			result[key] = make(map[string]uint32)
		}
		for _, value := range values {
			result[key][value] = result[key][value] + 1
		}
	}
	return result
}
