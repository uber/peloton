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

package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test extraction of IP and port from Agent PID
func TestExtractIPAndPortFromMesosPid(t *testing.T) {
	testcases := []struct {
		pid, ip, port string
		err           error
	}{
		{pid: "slave(1)@1.2.3.4:9090", ip: "1.2.3.4", port: "9090"},
		{pid: "slave(1)@2.3.4.5", ip: "2.3.4.5"},
		{
			pid: "slave(1)@:90",
			err: fmt.Errorf("Invalid Agent PID: slave(1)@:90"),
		},
		{pid: "slave(1)@ ", err: fmt.Errorf("Invalid Agent PID: slave(1)@ ")},
		{pid: " @1.2.3.4", err: fmt.Errorf("Invalid Agent PID: @1.2.3.4")},
		{pid: "slave(1)@", err: fmt.Errorf("Invalid Agent PID: slave(1)@")},
		{pid: "@1.2.3.4", err: fmt.Errorf("Invalid Agent PID: @1.2.3.4")},
		{pid: "badpid", err: fmt.Errorf("Invalid Agent PID: badpid")},
	}
	for _, tc := range testcases {
		ip, port, err := ExtractIPAndPortFromMesosAgentPID(tc.pid)
		if tc.err == nil {
			assert.NoError(t, err)
			assert.Equal(t, tc.ip, ip, tc.pid)
			assert.Equal(t, tc.port, port, tc.pid)
		} else {
			assert.Error(t, err)
		}
	}
}
