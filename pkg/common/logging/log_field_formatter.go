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

import log "github.com/sirupsen/logrus"

// LogFieldFormatter add fields provided in each log message.
// It provides a way to send log with default fields
type LogFieldFormatter struct {
	log.Fields
	log.Formatter
}

// Format is called by logrus and returns the formatted string.
// It adds the log fields provided to the log entry.
func (f *LogFieldFormatter) Format(entry *log.Entry) ([]byte, error) {
	for k, v := range f.Fields {
		// do not overwrite non-default fields
		if _, ok := entry.Data[k]; !ok {
			entry.Data[k] = v
		}
	}

	return f.Formatter.Format(entry)
}
