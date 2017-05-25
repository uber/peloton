package logmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	log "github.com/Sirupsen/logrus"
)

const (
	slaveStateURL      = "http://%s:5051/state"
	slaveFileBrowseURL = "http://%s:5051/files/browse?path=%s"
	pelotonRole        = "peloton"
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

type executorInfo struct {
	ID        string `json:"id"`
	Directory string `json:"directory"`
}

type slaveStateResponse struct {
	Frameworks []struct {
		Role               string         `json:"role"`
		Executors          []executorInfo `json:"executors"`
		CompletedExecutors []executorInfo `json:"completed_executors"`
	} `json:"frameworks"`
}

type filePath struct {
	Path string `json:"path"`
}

var (
	errTaskExecutorNotFound = errors.New("task executor not found in slave state response")
)

// ListSandboxFilesPaths returns the list of logs url under sandbox directory for given task.
func (l *LogManager) ListSandboxFilesPaths(hostname string, taskID string) ([]string, error) {
	var result []string
	slaveStateEndpointURL := fmt.Sprintf(slaveStateURL, hostname)
	sandboxDir, err := l.getTaskSandboxDirectory(slaveStateEndpointURL, taskID)
	if err != nil {
		return result, err
	}

	slaveFileBrowseEndpointURL := fmt.Sprintf(slaveFileBrowseURL, hostname, sandboxDir)
	logFiles, err := l.listTaskLogFiles(slaveFileBrowseEndpointURL)
	if err != nil {
		return result, err
	}

	return logFiles, nil
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

// getTaskSandboxDirectory returns sandbox directory for given taskID on given host.
func (l *LogManager) getTaskSandboxDirectory(stateURL string, taskID string) (string, error) {
	var result string
	resp, err := l.client.Get(stateURL)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("slave query response not ok: %v", resp)
	}

	var slaveResp slaveStateResponse
	if err = json.NewDecoder(resp.Body).Decode(&slaveResp); err != nil {
		return result, fmt.Errorf("slave query response decode failure: %v", resp)
	}

	var taskExecutor *executorInfo
	for _, framework := range slaveResp.Frameworks {
		if framework.Role != pelotonRole {
			continue
		}

		for _, executor := range framework.Executors {
			if executor.ID == taskID {
				taskExecutor = &executor
				break
			}
		}

		if taskExecutor == nil {
			for _, executor := range framework.CompletedExecutors {
				if executor.ID == taskID {
					taskExecutor = &executor
					break
				}
			}
		}
	}

	if taskExecutor == nil {
		log.WithField("url", stateURL).
			WithField("task_id", taskID).
			Error("task executors not found in slave state response")
		return result, errTaskExecutorNotFound
	}

	return taskExecutor.Directory, nil
}
