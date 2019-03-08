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

package logging

import (
	"encoding/base64"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	testPath      = "/tmp/testpath"
	testSecretStr = "my-secret"
)

// TestDbStatementsFormatting tests that logs containing DB statements and UQL
// queries are filtered and those that contain secret are redacted
func TestDbStatementsFormatting(t *testing.T) {
	formatter := SecretsFormatter{&logrus.JSONFormatter{}}
	b, err := formatter.Format(
		logrus.WithField(common.DBStmtLogField, "INSERT INTO secret_info where"))
	assert.NoError(t, err)
	assert.NotContains(t, string(b), "secret_info")
	assert.Contains(t, string(b), redactedStr)

	// if formatter sees secret_info as part of the DBUqlLogField, it will
	// redact the subsequent DBArgsLogField field in the entry.
	b, err = formatter.Format(
		logrus.WithFields(logrus.Fields{
			common.DBUqlLogField:  "INSERT INTO secret_info where",
			common.DBArgsLogField: "/tmp/secret-path, bXktc2VjcmV0"}))
	assert.NoError(t, err)
	assert.NotContains(t, string(b), "secret_info")
	assert.Contains(t, string(b), redactedStr)
}

// validate that the byte buffer does not have secret data
func validateSecretFormatting(s string, t *testing.T) {
	// make sure the secret path is kept as it is and is not redacted
	assert.Contains(t, s, testPath)
	// make sure the log string contains redactedStr and not the original secret
	assert.NotContains(t, s,
		base64.StdEncoding.EncodeToString([]byte(testSecretStr)))
	assert.Contains(t, s,
		base64.StdEncoding.EncodeToString([]byte(redactedStr)))
}

// TestLaunchableTasksFormatting tests that logs containing LaunchableTask or
// LaunchTasksRequest are filtered and if a taskconfig contains secret volume,
// the secret data is redacted in the log
func TestLaunchableTasksFormatting(t *testing.T) {
	// setup LaunchableTask, a list of LaunchableTask and LaunchTasksRequest
	// such that the task config contains secret volume
	launchableTaskWithSecret := &hostsvc.LaunchableTask{
		Config: &task.TaskConfig{
			Container: &mesos.ContainerInfo{
				Volumes: []*mesos.Volume{
					util.CreateSecretVolume(testPath, testSecretStr),
				},
			},
		},
	}
	launchableTasksList := []*hostsvc.LaunchableTask{
		launchableTaskWithSecret,
	}
	launchableTasksRequest := &hostsvc.LaunchTasksRequest{
		Tasks: launchableTasksList,
	}

	formatter := SecretsFormatter{&logrus.JSONFormatter{}}

	// launchableTasksRequest contains secret data, it should be redacted
	b, err := formatter.Format(logrus.WithField("req", launchableTasksRequest))
	assert.NoError(t, err)
	validateSecretFormatting(string(b), t)

	b, err = formatter.Format(logrus.WithField("list", launchableTasksList))
	assert.NoError(t, err)
	validateSecretFormatting(string(b), t)

	b, err = formatter.Format(
		logrus.WithField("task", launchableTaskWithSecret))
	assert.NoError(t, err)
	validateSecretFormatting(string(b), t)
}
