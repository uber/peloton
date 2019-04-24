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
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/uber/peloton/pkg/common"
)

const (
	_slaveSandboxDir    = "%s/slaves/%s/frameworks/%s/executors/%s/runs/latest"
	_slaveFileBrowseURL = "http://%s:%s/files/browse?path=%s"
)

// TODO: (varung) Move this component to HostManger

// LogManager log manager, is used to access sandbox files under mesos agent executor run directory.
// It also contains log files.
type LogManager interface {

	// ListSandboxFilesPaths lists all the sandbox files in the mesos agent executor run directory.
	ListSandboxFilesPaths(mesosAgentWorDir,
		frameworkID,
		hostname,
		port,
		agentID,
		taskID string) ([]string, error)
}

// logManager is a wrapper to collect logs location by talking to mesos agents.
type logManager struct {
	client *http.Client
}

// NewLogManager returns a logManager instance.
func NewLogManager(client *http.Client) LogManager {
	return &logManager{
		client: client,
	}
}

type filePath struct {
	Path string `json:"path"`
}

// ListSandboxFilesPaths returns the list of logs url under sandbox directory for given task.
func (l *logManager) ListSandboxFilesPaths(
	mesosAgentWorDir, frameworkID, hostname, port,
	agentID, taskID string) ([]string, error) {
	slaveBrowseURL := getSlaveFileBrowseEndpointURL(
		mesosAgentWorDir,
		frameworkID,
		hostname,
		port,
		agentID,
		taskID)

	result, err := listTaskLogFiles(l.client, slaveBrowseURL)
	if err != nil {
		// If listing the files for sandbox with executorID == taskID
		// failed, indicates that task was launched by thermos executor
		// and thermos executor ID has a prefix of `thermos`
		slaveBrowseURL := getSlaveFileBrowseEndpointURL(
			mesosAgentWorDir,
			frameworkID,
			hostname,
			port,
			agentID,
			common.PelotonAuroraBridgeExecutorIDPrefix+taskID)

		result, err = listTaskLogFiles(l.client, slaveBrowseURL)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

func getSlaveFileBrowseEndpointURL(mesosAgentWorDir, frameworkID,
	hostname, port, agentID, taskID string) string {
	sandboxDir := fmt.Sprintf(
		_slaveSandboxDir,
		mesosAgentWorDir,
		agentID,
		frameworkID,
		taskID)
	return fmt.Sprintf(_slaveFileBrowseURL, hostname, port, sandboxDir)
}

// listTaskLogFiles list logs files paths under given sandbox directory.
func listTaskLogFiles(client *http.Client, fileURL string) ([]string, error) {

	var result []string
	resp, err := client.Get(fileURL)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("HTTP GET failed for %s: %v", fileURL, resp)
	}

	var slaveResp []filePath
	if err = json.NewDecoder(resp.Body).Decode(&slaveResp); err != nil {
		return result,
			fmt.Errorf("Failed to decode response for %s: %v", fileURL, resp)
	}

	for _, file := range slaveResp {
		result = append(result, file.Path)
	}
	return result, nil
}
