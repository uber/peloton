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

// TODO: (varung) Move this component to HostManger

// LogManager log manager, is used to access sandbox files under mesos agent executor run directory.
// It also contains log files.
type LogManager interface {

	// ListSandboxFilesPaths lists all the sandbox files in the mesos agent executor run directory.
	ListSandboxFilesPaths(mesosAgentWorDir, frameworkID, hostname, agentID, taskID string) ([]string, error)
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
	mesosAgentWorDir, frameworkID, hostname, agentID, taskID string) ([]string, error) {
	slaveBrowseURL := getSlaveFileBrowseEndpointURL(mesosAgentWorDir, frameworkID, hostname, agentID, taskID)

	result, err := listTaskLogFiles(l.client, slaveBrowseURL)
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
func listTaskLogFiles(client *http.Client, fileURL string) ([]string, error) {

	var result []string
	resp, err := client.Get(fileURL)
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
