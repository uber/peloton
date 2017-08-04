package logmanager

import (
	"encoding/json"
	"fmt"
	"net/http"
)

const (
	_slaveSandboxDir    = "%s/slaves/%s/frameworks/%s/executors/%s/runs/latest"
	_slaveFileBrowseURL = "http://%s:5051/files/browse?path=%s"
)

// LogManager is a wrapper to collect logs location by talking to mesos agents.
type LogManager struct {
	client *http.Client
}

// NewLogManager returns a logManager instance.
func NewLogManager(client *http.Client) *LogManager {
	return &LogManager{
		client: client,
	}
}

type filePath struct {
	Path string `json:"path"`
}

// ListSandboxFilesPaths returns the list of logs url under sandbox directory for given task.
func (l *LogManager) ListSandboxFilesPaths(
	mesosAgentWorDir, frameworkID, hostname, agentID, taskID string) ([]string, error) {
	slaveBrowseURL := getSlaveFileBrowseEndpointURL(mesosAgentWorDir, frameworkID, hostname, agentID, taskID)

	result, err := l.listTaskLogFiles(slaveBrowseURL)
	if err != nil {
		return result, err
	}

	return result, nil
}

func getSlaveFileBrowseEndpointURL(mesosAgentWorDir, frameworkID, hostname, agentID, taskID string) string {
	sandboxDir := fmt.Sprintf(_slaveSandboxDir, mesosAgentWorDir, agentID, frameworkID, taskID)
	return fmt.Sprintf(_slaveFileBrowseURL, hostname, sandboxDir)
}

// listTaskLogFiles list logs files paths under given sandbox directory.
func (l *LogManager) listTaskLogFiles(fileURL string) ([]string, error) {

	var result []string
	resp, err := l.client.Get(fileURL)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("slave filesbrowse response not ok: %v", resp)
	}

	var slaveResp []filePath
	if err = json.NewDecoder(resp.Body).Decode(&slaveResp); err != nil {
		return result, fmt.Errorf("slave filesbrowse response decode failure: %v", resp)
	}

	for _, file := range slaveResp {
		result = append(result, file.Path)
	}
	return result, nil
}
