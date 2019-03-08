package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	testClusterName        = "cluster1"
	testInvalidClusterName = "invalidClusterName"
	testZKURL              = "abc:0000"
)

var (
	zkJSONBytes = []byte("{\"clusters\":[{\"clusterName\":\"cluster1\"," +
		"\"zkURL\":\"abc:0000\"}]}")
	zkInvalidJSONBytes = []byte("a,b")
)

// TestGetValidZkURLValidClusterName tests valid file name and valid cluster
// name
func TestGetValidZkURLValidClusterName(t *testing.T) {
	zkURL, err := GetZkInfoFromClusterName(testClusterName, zkJSONBytes)
	assert.Nil(t, err)
	expected := testZKURL
	assert.Equal(t, expected, string(zkURL))
}

// TestInvalidClusterName tests cluster is not in the config file
func TestInvalidClusterName(t *testing.T) {
	zkURL, err := GetZkInfoFromClusterName(testInvalidClusterName, zkJSONBytes)
	assert.NotNil(t, err)
	assert.EqualValues(t, zkURL, "", "response "+
		"should be empty string")
	expectedErr := "cannot find the corresponding zk url " +
		"for " + testInvalidClusterName
	assert.EqualError(t, err, expectedErr)
}

// TestInvalidJSONFile tests invalid cluster json file
func TestInvalidJSONFile(t *testing.T) {
	zkURL, err := GetZkInfoFromClusterName(testClusterName,
		zkInvalidJSONBytes)
	assert.NotNil(t, err)
	assert.EqualValues(t, zkURL, "", "response "+
		"should be empty string")
	expectedErr := "invalid json string: invalid character 'a' looking for beginning of value"
	assert.EqualError(t, err, expectedErr)
}
