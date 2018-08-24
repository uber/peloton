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
