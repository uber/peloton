package logging

import (
	"strings"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/common"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

// SecretsFormatter scrubs sensitive information from logs and formats logs into
// parsable json.
type SecretsFormatter struct {
	*log.JSONFormatter
}

const redactedStr = "REDACTED"

// redactSecrets redacts secret data in task config
func redactSecrets(taskConfig *task.TaskConfig) {
	for _, volume := range taskConfig.GetContainer().GetVolumes() {
		if volume.GetSource().GetType() == mesos.Volume_Source_SECRET &&
			volume.GetSource().GetSecret().GetValue().GetData() != nil {
			volume.GetSource().GetSecret().GetValue().Data = []byte(redactedStr)
		}
	}
}

// Format is called by logrus and returns the formatted string.
// It looks for secrets data in each entry and redacts it.
func (f *SecretsFormatter) Format(entry *log.Entry) ([]byte, error) {
	for k, v := range entry.Data {
		// look for taskConfig, secret, secret_info string
		switch v := v.(type) {
		case string:
			// filter DB statement so it doesn't contain secret_info
			if (k == common.DBStmtLogField || k == common.DBUqlLogField) &&
				strings.Contains(v, "secret_info") {
				entry.Data[k] = redactedStr
				// CQL Query is on secret_info. Check for the DBArgsLogField
				// which contains actual secret data and redact it
				if _, ok := entry.Data[common.DBArgsLogField]; ok {
					// This field will vary depending on insert or update order
					// It will be in this format:
					// {"args":["5baa5b2d-3856-4112-a35f-04a0c21ed2d6",
					// "3e3fffe1-efda-4aea-a477-d2106e13710a",
					// "/tmp/secret-path","YzI4Z2JYVmphQ0JuWVhKaVlXZGw=",
					// "2018-06-12T22:22:10.002332",0,true]...rest of the fields
					// so we will replace the entire field with redactedStr
					entry.Data[common.DBArgsLogField] = redactedStr
				}
			}
		case *hostsvc.LaunchTasksRequest:
			// The hostsvc.LaunchTasksRequest will contain populated secrets when
			// tasks are being launched from launcher. This check makes sure that
			// these secrets are not logged from jobmgr as well as hostmgr
			clonedLaunchRequest := proto.Clone(v).(*hostsvc.LaunchTasksRequest)
			for _, task := range clonedLaunchRequest.GetTasks() {
				redactSecrets(task.GetConfig())
			}
			entry.Data[k] = clonedLaunchRequest
		case *hostsvc.LaunchableTask:
			// The hostsvc.LaunchableTask will contain populated secrets when
			// tasks are being launched from launcher. This check makes sure that
			// these secrets are not logged from jobmgr as well as hostmgr
			clonedLaunchableTask := proto.Clone(v).(*hostsvc.LaunchableTask)
			redactSecrets(clonedLaunchableTask.GetConfig())
			entry.Data[k] = clonedLaunchableTask
		case []*hostsvc.LaunchableTask:
			newList := []*hostsvc.LaunchableTask{}
			for _, task := range v {
				clonedLaunchableTask := proto.Clone(task).(*hostsvc.LaunchableTask)
				redactSecrets(clonedLaunchableTask.GetConfig())
				newList = append(newList, clonedLaunchableTask)
			}
			entry.Data[k] = newList
		}
	}
	return f.JSONFormatter.Format(entry)
}
