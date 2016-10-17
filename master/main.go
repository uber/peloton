package main

import (
	"fmt"
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/transport"
	"github.com/yarpc/yarpc-go/transport/http"
	"golang.org/x/net/context"

	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/master/job"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/master/offer"
	"code.uber.internal/infra/peloton/master/task"
	"code.uber.internal/infra/peloton/master/upgrade"
	"code.uber.internal/infra/peloton/scheduler"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/util"
	myarpc "code.uber.internal/infra/peloton/yarpc"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
	"strconv"
	"time"
)

const (
	getStreamIdRetryTimes = 30
	getStreamIdRetrySleep = 10 * time.Second
)

// Simple request interceptor which logs the request summary
type requestLogInterceptor struct{}

func (requestLogInterceptor) Handle(
	ctx context.Context,
	opts transport.Options,
	req *transport.Request,
	resw transport.ResponseWriter,
	handler transport.Handler) error {

	log.Infof("Received a %s request from %s", req.Procedure, req.Caller)
	return handler.Handle(ctx, opts, req, resw)
}

// peloton master leader would create mesos inbound, which would subscribe to meso master
func becomeLeader(mInbound transport.Inbound, offerQueue util.OfferQueue, store *mysql.MysqlJobStore, cfg *AppConfig) yarpc.Dispatcher {
	mesosInDispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "peloton-mesos-inbound",
		Inbounds: []transport.Inbound{
			mInbound,
		},
		Interceptor: yarpc.Interceptors(requestLogInterceptor{}),
	})

	mesos.InitManager(mesosInDispatcher, &cfg.Mesos, store)
	upgrade.InitManager(mesosInDispatcher)
	offer.InitManager(mesosInDispatcher, offerQueue)
	task.InitTaskStateUpdateManager(mesosInDispatcher, store, store)
	return mesosInDispatcher
}

// peloton master follower would try to read db for the mesos stream id
func getStreamId(store *mysql.MysqlJobStore, cfg *AppConfig) (string, error) {
	var errMsg string
	for i := 0; i < getStreamIdRetryTimes; i++ {
		mesosStreamId, err := store.GetMesosStreamId(cfg.Mesos.Framework.Name)
		if err == nil && mesosStreamId != "" {
			return mesosStreamId, err
		}
		errMsg = fmt.Sprintf("Could not get mesos stream id from db, framework %v, error= %v", cfg.Mesos.Framework.Name, err)
		log.Errorf(errMsg)
		log.Infof("attempt %v, sleep 100 sec", i)
		time.Sleep(getStreamIdRetrySleep)
	}
	return "", fmt.Errorf(errMsg)
}

func main() {
	var cfg AppConfig
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

	// connect to mysql DB
	if err := cfg.DbConfig.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}

	// Migrate DB if necessary
	if errs := cfg.DbConfig.AutoMigrate(); errs != nil {
		log.Fatalf("Could not migrate database: %+v", errs)
	}

	// TODO: Load framework ID from DB
	offerQueue := util.NewMemLocalOfferQueue("LocalOfferQueue")
	taskQueue := util.NewMemLocalTaskQueue("LocalTaskQueue")

	// Creates the peloton rpc inbound
	pelotonInDispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "peloton-master",
		Inbounds: []transport.Inbound{
			http.NewInbound(":" + strconv.Itoa(cfg.Master.Port)),
		},
		Interceptor: yarpc.Interceptors(requestLogInterceptor{}),
	})
	store := mysql.NewMysqlJobStore(cfg.DbConfig.Conn)
	f := mesos.NewSchedulerDriver(&cfg.Mesos.Framework, nil)
	var mesosStreamId string
	if cfg.Master.Leader {
		mInbound := mhttp.NewInbound(cfg.Mesos.HostPort, f)
		log.Infof("Becoming leader, creating mesosInDispatcher")
		mesosInDispatcher := becomeLeader(mInbound, offerQueue, store, &cfg)
		if err := mesosInDispatcher.Start(); err != nil {
			log.Fatalf("Could not start rpc server for mesosInDispatcher: %v", err)
		}
		mesosStreamId = mInbound.GetMesosStreamId()
		err = store.SetMesosStreamId(cfg.Mesos.Framework.Name, mesosStreamId)
		if err != nil {
			log.Errorf("failed to SetMesosStreamId %v %v, err=%v", cfg.Mesos.Framework.Name, mesosStreamId, err)
		}
		log.Info("mesosInDispatcher started with stream id %v", mesosStreamId)

	} else {
		mesosStreamId, err = getStreamId(store, &cfg)
		if err != nil {
			log.Fatalf("Could not get mesos stream id from db: %v", err)
		}
	}

	mOutbound := mhttp.NewOutbound(cfg.Mesos.HostPort, f, mesosStreamId)
	outDispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:        "peloton-mesos-outbound",
		Outbounds:   transport.Outbounds{"peloton-master": mOutbound},
		Interceptor: yarpc.Interceptors(requestLogInterceptor{}),
	})

	job.InitManager(pelotonInDispatcher, store, store, taskQueue)

	// Create mesos outbound
	launcher := task.InitManager(pelotonInDispatcher, store, store, offerQueue, taskQueue, myarpc.NewMesoCaller(mOutbound))
	scheduler.InitManager(offerQueue, taskQueue, launcher)

	if err := outDispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server for outDispatcher: %v", err)
	}
	log.Info("outDispatcher started")

	if err := pelotonInDispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server for pelotonInDispatcher: %v", err)
	}
	log.Info("pelotonInDispatcher started")

	log.Info("Started rpc server")
	select {}
}
