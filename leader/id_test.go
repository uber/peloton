package leader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServiceInstance(t *testing.T) {
	additionalEndpoints := make(map[string]Endpoint, 1)

	endpoint := Endpoint{
		Host: "10.102.141.24",
		Port: 8082,
	}
	aEndpoint := Endpoint{
		Host: "10.102.141.24",
		Port: 8082,
	}
	additionalEndpoints["http"] = aEndpoint

	serviceInstance := NewServiceInstance(endpoint, additionalEndpoints)

	// Expected aurora znode format
	expected := "{\"serviceEndpoint\":{\"host\":\"10.102.141.24\",\"port\":8082},\"additionalEndpoints\":{\"http\":{\"host\":\"10.102.141.24\",\"port\":8082}},\"status\":\"ALIVE\"}"
	assert.Equal(t, expected, serviceInstance)
}
