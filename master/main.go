package main

import (
	gen "code.uber.internal/infra/peloton/.gen/go/goal_state"
	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/metrics"
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/tchannel"
    
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

type appConfig struct {
    Logging  log.Configuration
    Metrics  metrics.Configuration
    Sentry   log.SentryConfiguration
    Verbose  bool
    TChannel xtchannel.Configuration
}

func main() {
	var cfg appConfig
	if err := config.Load(&cfg); err != nil {
		log.Fatalf("Error initializing configuration: %s", err)
	}
	log.Configure(&cfg.Logging, cfg.Verbose)
	log.ConfigureSentry(&cfg.Sentry)

	metrics, err := cfg.Metrics.New()
	if err != nil {
		log.Fatalf("Could not connect to metrics: %v", err)
	}
	metrics.Counter("boot").Inc(1)

	if _, err := cfg.TChannel.New("peloton-master", metrics, registerHandlers); err != nil {
		log.Fatalf("TChannel.New failed: %v", err)
	}

	// Block forever.
	select {}
}

func registerHandlers(ch *tchannel.Channel, server *thrift.Server) {
	server.Register(gen.NewTChanGoalStateManagerServer(goalStateManager{}))
}

type goalStateManager struct{}

func (goalStateManager)	QueryActualStates(
    ctx thrift.Context, queries []*gen.StateQuery) ([]*gen.ActualState, error) {
    return make([]*gen.ActualState, 0), nil
}

func (goalStateManager)	QueryGoalStates(
    ctx thrift.Context, queries []*gen.StateQuery) ([]*gen.GoalState, error) {
    return  make([]*gen.GoalState, 0), nil
}

func (goalStateManager) SetGoalStates(
    ctx thrift.Context, states []*gen.GoalState) error {
    return nil
}

func (goalStateManager) UpdateActualStates(
    ctx thrift.Context, states []*gen.ActualState) error {
    return nil
}

