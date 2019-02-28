// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	_testFrameworkID  = "test-framework-id"
	_testAgentID      = "test-agent-id"
	_testTaskID       = "test-task-id"
	_testHostname     = "test-hostname"
	_testPort         = "31002"
	_testMesosWorkDir = "/var/lib/mesos/agent"
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

func (suite *LogManagerTestSuite) TestListTaskLogFilesNoClient() {
	ts := httptest.NewServer(slaveMux())
	defer ts.Close()

	_, err := listTaskLogFiles(&http.Client{
		Timeout: 10 * time.Second,
	}, "UnexistFile")

	suite.Error(err)

	_, err = listTaskLogFiles(&http.Client{
		Timeout: 10 * time.Second,
	}, ts.URL+"/failed")
	suite.Error(err)

	_, err = listTaskLogFiles(&http.Client{
		Timeout: 10 * time.Second,
	}, ts.URL+"/nonjson")
	suite.Error(err)
}

func (suite *LogManagerTestSuite) TestListSandboxFilesPaths() {
	lm := &logManager{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	_, err := lm.ListSandboxFilesPaths(
		_testMesosWorkDir,
		_testFrameworkID,
		_testHostname,
		_testPort,
		_testAgentID,
		_testTaskID)
	suite.Error(err)
}

func (suite *LogManagerTestSuite) TestGetSlaveFileBrowseEndpointURL() {
	sandboxDir := getSlaveFileBrowseEndpointURL(
		_testMesosWorkDir, _testFrameworkID, _testHostname, _testPort,
		_testAgentID, _testTaskID)
	suite.Equal(
		"http://test-hostname:31002/files/browse?path="+
			"/var/lib/mesos/agent/slaves/test-agent-id/frameworks"+
			"/test-framework-id/executors/test-task-id/runs/latest",
		sandboxDir)
}

var (
	_slaveFileBrowseStr = `[{"path": "/var/lib/path1"}, {"path": "/var/lib/path2"}]`
	_NonJSONResponse    = `error`
)

func slaveMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/files/browse", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, _slaveFileBrowseStr)
		return
	})

	mux.HandleFunc("/failed", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		return
	})

	mux.HandleFunc("/nonjson", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, _NonJSONResponse)
		return
	})

	return mux
}
