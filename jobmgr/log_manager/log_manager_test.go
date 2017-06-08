package logmanager

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type LogManagerTestSuite struct {
	suite.Suite

	logManager *LogManager
}

func (suite *LogManagerTestSuite) SetupTest() {
	suite.logManager = NewLogManager(
		&http.Client{
			Timeout: 10 * time.Second,
		},
	)
}

func (suite *LogManagerTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestLogManager(t *testing.T) {
	suite.Run(t, new(LogManagerTestSuite))
}

func (suite *LogManagerTestSuite) TestGetTaskSandboxDirectory() {
	ts := httptest.NewServer(slaveMux())
	defer ts.Close()

	sandboxDir, err := suite.logManager.getTaskSandboxDirectory(ts.URL+"/state", "testTaskID")
	suite.NoError(err)
	suite.Equal(sandboxDir, "/var/lib/test")
}

func (suite *LogManagerTestSuite) TestGetTaskSandboxDirectoryInCompletedFrameworks() {
	ts := httptest.NewServer(slaveMux())
	defer ts.Close()

	sandboxDir, err := suite.logManager.getTaskSandboxDirectory(ts.URL+"/state", "testTaskID2")
	suite.NoError(err)
	suite.Equal(sandboxDir, "/var/lib/test2")
}

func (suite *LogManagerTestSuite) TestGetTaskSandboxDirectoryNotFound() {
	ts := httptest.NewServer(slaveMux())
	defer ts.Close()

	sandboxDir, err := suite.logManager.getTaskSandboxDirectory(ts.URL+"/state", "notExistTaskID")
	suite.Equal(err, errTaskExecutorNotFound)
	suite.Empty(sandboxDir, "")
}

func (suite *LogManagerTestSuite) TestListTaskLogFiles() {
	ts := httptest.NewServer(slaveMux())
	defer ts.Close()

	filePaths, err := suite.logManager.listTaskLogFiles(ts.URL + "/files/browse?path=" + "testPath")
	suite.NoError(err)
	suite.Equal(filePaths, []string{"/var/lib/path1", "/var/lib/path2"})
}

var (
	slaveStateStr = `{"frameworks": [{"role": "peloton", "executors": [{"id": "testTaskID", "directory": "/var/lib/test"}]}],
	"completed_frameworks": [{"role": "peloton", "executors": [{"id": "testTaskID2", "directory": "/var/lib/test2"}]}]}`
	slaveFileBrowseStr = `[{"path": "/var/lib/path1"}, {"path": "/var/lib/path2"}]`
)

func slaveMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, slaveStateStr)
		return
	})

	mux.HandleFunc("/files/browse", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, slaveFileBrowseStr)
		return
	})

	return mux
}
