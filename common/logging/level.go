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
	"fmt"
	"net/http"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
)

const (
	// LevelOverwrite is the default endpoint for overwrite level handler.
	LevelOverwrite = "/logging-level"

	_level    = "level"
	_duration = "duration"
	_usage    = "usage: GET `/logging-level?level=[info|debug]&duration=<duration>`"
)

var (
	_loggingLevel atomic.Int32
)

func getParams(names []string, r *http.Request) (map[string]string, error) {
	result := make(map[string]string)
	values := r.URL.Query()
	var missing []string
	for _, name := range names {
		v, ok := values[name]
		if !ok || len(v) == 0 {
			missing = append(missing, name)
			continue
		}

		result[name] = v[0]
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("Required params not set: %s", strings.Join(missing, ","))
	}

	return result, nil
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintln(w, err.Error())
	fmt.Fprintln(w, _usage)
}

// LevelOverwriteHandler returns a handler for overwrite logging level for a duration.
// If this hanlder is invoked multiple times, the earliest finish time based on duration will reset the logging level.
func LevelOverwriteHandler(initialLevel log.Level) func(http.ResponseWriter, *http.Request) {
	_loggingLevel.Store(int32(initialLevel))
	log.SetLevel(initialLevel)
	return func(w http.ResponseWriter, r *http.Request) {
		params, err := getParams([]string{_level, _duration}, r)
		if err != nil {
			writeError(w, err)
			return
		}

		newLevel, err := log.ParseLevel(params[_level])
		if err != nil {
			writeError(w, err)
			return
		}

		if newLevel != log.InfoLevel && newLevel != log.DebugLevel {
			writeError(w, fmt.Errorf("New Level %s is not info or debug", params[_level]))
			return
		}

		duration, err := time.ParseDuration(params[_duration])
		if err != nil {
			writeError(w, err)
			return
		}

		log.WithFields(log.Fields{
			"new_level": newLevel,
			"duration":  duration,
		}).Info("Setting log level to new level")
		log.SetLevel(newLevel)

		timer := time.NewTimer(duration)
		go func() {
			<-timer.C
			level := log.Level(_loggingLevel.Load())
			log.WithField("initial_level", level).Info("Resetting log level after timer")
			log.SetLevel(level)
		}()

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Level changed to %s for the next %v.\n", params[_level], duration)
	}
}
