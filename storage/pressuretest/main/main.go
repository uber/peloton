package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/cassandra"
	"code.uber.internal/infra/peloton/storage/cassandra/impl"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"gopkg.in/alecthomas/kingpin.v2"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

var (
	app           = kingpin.New("peloton", "Util to pressure test C* store")
	cassandraHost = app.Flag(
		"cassandra-hosts", "Cassandra hosts").
		Short('h').
		String()

	cassandraPort = app.Flag(
		"cassandra-port", "Cassandra port").
		Short('p').
		Default("9042").
		Int()

	consistency = app.Flag(
		"consistency", "data consistency").
		Short('c').
		Default("LOCAL_QUORUM").
		String()

	workers = app.Flag(
		"workers", "number of workers").
		Short('w').
		Int()

	storeName = app.Flag(
		"store", "store").
		Short('s').
		String()

	taskBatchsize = app.Flag(
		"batch", "task batch size per worker").
		Short('t').
		Int()

	validateUpdate = app.Flag(
		"validate_update", "validate updated task state").
		Short('v').
		Bool()
	//TODO: controllable QPS
)

// Util to generate load test to C* using peloton taskStore code
func main() {
	run(os.Args[1:])
}

func run(args []string) []error {
	kingpin.MustParse(app.Parse(args))

	// TODO: investigate how to get order statistics for the latency values
	// For now, we can still read latency numbers from graphite dashboards
	// https://graphite.uberinternal.com/grafana2/dashboard/db/cassandra-mesos-irn
	rootScope, scopeCloser, _ := metrics.InitMetricScope(
		&metrics.Config{},
		"perfTest",
		metrics.TallyFlushInterval)
	defer scopeCloser.Close()

	rootScope.Timer("CreateTask").Start()
	rootScope.Timer("UpdateTask").Start()
	rootScope.Timer("GetTask").Start()

	conf := migrateSchemas()

	return runTest(conf, rootScope, *workers, *taskBatchsize)
}

func migrateSchemas() *cassandra.Config {
	// TODO: add logic to create the store(keyspace) if not exist
	cassandraHosts := strings.Split(*cassandraHost, ",")
	log.Debugf("c* hosts %v %v", cassandraHost, cassandraHosts)
	conf := cassandra.Config{
		CassandraConn: &impl.CassandraConn{
			ContactPoints: cassandraHosts,
			Port:          *cassandraPort,
			CQLVersion:    "3.4.2",
			MaxGoRoutines: 1000,
			Timeout:       10 * time.Second,
			Consistency:   *consistency,
		},
		StoreName:          *storeName,
		Migrations:         "migrations",
		MaxBatchSize:       20,
		MaxParallelBatches: 10,
	}

	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get PWD, err=%v", err)
	}

	for !strings.HasSuffix(path.Clean(dir), "/peloton") && len(dir) > 1 {
		dir = path.Join(dir, "..")
	}

	conf.Migrations = path.Join(dir, "storage", "cassandra", conf.Migrations)
	log.Infof("pwd=%v migration path=%v", dir, conf.Migrations)
	if errs := conf.AutoMigrate(); errs != nil {
		panic(fmt.Sprintf("%+v", errs))
	}
	return &conf
}

// in each go routine, create - read -> update some tasks, track latency numbers
func runTest(conf *cassandra.Config, rootScope tally.Scope, workers int, batchSize int) []error {
	taskStore, err := cassandra.NewStore(conf, rootScope)
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}
	jobID := uuid.New()
	wg := &sync.WaitGroup{}
	lock := &sync.Mutex{}
	lock.Lock()
	var errors []error
	lock.Unlock()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			log.Infof("worker %d started", index)
			for j := 0; j < batchSize; j++ {
				instanceID := uint32(index*batchSize + j)
				err := createTask(taskStore, jobID, instanceID, rootScope)
				if err != nil {
					for _, stateVal := range task.TaskState_value {
						err = updateTaskState(taskStore, jobID, instanceID, task.TaskState(stateVal), rootScope)
						if err != nil {
							break
						}
					}
				}
				if err != nil {
					lock.Lock()
					errors = append(errors, err)
					lock.Unlock()
				}
			}
			defer wg.Done()
		}(i)
	}
	wg.Wait()
	log.Infof("completed test with jobID %v, with %v errors", jobID, len(errors))
	return errors
}

func createTask(taskStore storage.TaskStore, jobIDVal string, instance uint32, rootScope tally.Scope) error {
	var jobID = &peloton.JobID{Value: jobIDVal}
	tid := fmt.Sprintf("%s-%s", jobID, uuid.New())
	runtime := &task.RuntimeInfo{
		MesosTaskId: &mesos.TaskID{Value: &tid},
		State:       task.TaskState_INITIALIZED,
		Host:        fmt.Sprintf("host-%v", instance),
	}
	t := time.Now()
	err := taskStore.CreateTaskRuntime(context.Background(), jobID, instance, runtime, "test")
	d := time.Since(t)
	rootScope.Timer("CreateTask").Record(d)
	if err != nil {
		log.WithError(err).Error("Create task failed")
		return err
	}
	return nil
}

func updateTaskState(taskStore storage.TaskStore, jobIDVal string, instance uint32, state task.TaskState, rootScope tally.Scope) error {
	var jobID = &peloton.JobID{Value: jobIDVal}
	t := time.Now()
	taskInfo, err := taskStore.GetTaskForJob(context.Background(), jobID, instance)
	d := time.Since(t)
	rootScope.Timer("GetTask").Record(d)
	if err != nil {
		log.WithError(err).Error("Get task failed")
		return err
	}
	taskInfo[instance].GetRuntime().State = state
	t = time.Now()
	runtimes := make(map[uint32]*task.RuntimeInfo)
	runtimes[instance] = taskInfo[instance].Runtime
	err = taskStore.UpdateTaskRuntimes(context.Background(), &peloton.JobID{Value: jobIDVal}, runtimes)
	d = time.Since(t)
	rootScope.Timer("UpdateTask").Record(d)
	if err != nil {
		log.WithError(err).Error("update task failed")
		return err
	}
	if *validateUpdate == true {
		taskInfo, err = taskStore.GetTaskForJob(context.Background(), jobID, instance)
		if err != nil {
			log.WithError(err).Error("Get task failed")
			return err
		}
		// If updated task state is not read, wait for 100 msec and
		// try again.
		if taskInfo[instance].GetRuntime().State != state {
			log.WithField("instance", instance).
				WithField("expected state", state).
				WithField("actual state", taskInfo[instance].GetRuntime().State).
				Error("Task state not updated")

			time.Sleep(100 * time.Millisecond)
			taskInfo, err = taskStore.GetTaskForJob(context.Background(), jobID, instance)
			if err != nil {
				log.WithError(err).Error("Get task failed")
				return err
			}
			if taskInfo[instance].GetRuntime().State != state {
				log.WithField("instance", instance).
					WithField("expected state", state).
					WithField("actual state", taskInfo[instance].GetRuntime().State).
					Error("Task state not updated after 100ms")
			}
		}
	}
	return nil
}
