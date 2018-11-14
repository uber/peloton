package cli

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	testClusterName        = "pit1-preprod01"
	testInvalidClusterName = "invalidClusterName"
	testZKURL              = "zookeeper-mesos-preprod01.pit-irn-1.uberatc.net:2181"
)

// positive TC : valid file name and valid cluster name
func TestGetValidZkURLValidClusterName(t *testing.T) {
	zkURL, err := GetZkInfoFromClusterName(testClusterName)
	assert.Nil(t, err)
	expected := testZKURL
	assert.Equal(t, expected, string(zkURL))
}

// negative TC: cluster is not in the config file
func TestInvalidClusterName(t *testing.T) {
	zkURL, err := GetZkInfoFromClusterName(testInvalidClusterName)
	assert.NotNil(t, err)
	assert.EqualValues(t, zkURL, "", "response should be empty string")
	expectedErr := "cannot find the corresponding zkURL for " + testInvalidClusterName
	assert.EqualError(t, err, expectedErr)
}
