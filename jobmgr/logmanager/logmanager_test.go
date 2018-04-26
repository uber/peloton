package logmanager

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

const (
	_testFrameworkID = "test-framework-id"
	_testAgentID     = "test-agent-id"
	_testTaskID      = "test-task-id"
	_testHostname    = "test-hostname"
)

type LogManagerTestSuite struct {
	suite.Suite
}

func (suite *LogManagerTestSuite) SetupTest() {
	log.Debug("setup test")
}

func (suite *LogManagerTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestLogManager(t *testing.T) {
	suite.Run(t, new(LogManagerTestSuite))
}

func (suite *LogManagerTestSuite) TestListTaskLogFiles() {
	ts := httptest.NewServer(slaveMux())
	defer ts.Close()

	filePaths, err := listTaskLogFiles(&http.Client{
		Timeout: 10 * time.Second,
	}, ts.URL+"/files/browse?path="+"testPath")

	suite.NoError(err)
	suite.Equal(filePaths, []string{"/var/lib/path1", "/var/lib/path2"})
}

func (suite *LogManagerTestSuite) TestGetSlaveFileBrowseEndpointURL() {
	sandboxDir := getSlaveFileBrowseEndpointURL("/var/lib/mesos/agent", _testFrameworkID, _testHostname, _testAgentID, _testTaskID)
	suite.Equal(
		sandboxDir,
		"http://test-hostname:5051/files/browse?path=/var/lib/mesos/agent/slaves/test-agent-id/frameworks/test-framework-id/executors/test-task-id/runs/latest")
}

var (
	_slaveFileBrowseStr = `[{"path": "/var/lib/path1"}, {"path": "/var/lib/path2"}]`
)

func slaveMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/files/browse", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, _slaveFileBrowseStr)
		return
	})

	return mux
}
