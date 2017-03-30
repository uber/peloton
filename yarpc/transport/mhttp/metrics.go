package mhttp

import "github.com/uber-go/tally"

// Metrics hold all metrics related to mhttp.
type Metrics struct {
	Running tally.Gauge
	Stopped tally.Gauge

	LeaderChanges tally.Counter
	StartCount    tally.Counter
	StopCount     tally.Counter

	Frames tally.Counter

	ReadLineError    tally.Counter
	FrameLengthError tally.Counter
	LineLengthError  tally.Counter
	RecordIOError    tally.Counter
}

func newMetrics(parent tally.Scope) *Metrics {
	scope := parent.SubScope("mhttp")
	errScope := scope.SubScope("errors")
	return &Metrics{
		Running: scope.Gauge("running"),
		Stopped: scope.Gauge("stopped"),

		LeaderChanges: scope.Counter("leader_changes"),
		StartCount:    scope.Counter("start"),
		StopCount:     scope.Counter("stop"),

		Frames: scope.Counter("frames"),

		ReadLineError:    errScope.Counter("read_line"),
		FrameLengthError: errScope.Counter("frame_length"),
		LineLengthError:  errScope.Counter("line_length"),
		RecordIOError:    errScope.Counter("recordio"),
	}
}
