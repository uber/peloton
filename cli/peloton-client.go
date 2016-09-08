package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"
	"github.com/yarpc/yarpc-go/transport"
	"github.com/yarpc/yarpc-go/transport/http"
	"golang.org/x/net/context"
	"gopkg.in/yaml.v2"
	"peloton/job"
	"peloton/task"
)

const (
	Job     = "job"
	Task    = "task"
	Upgrade = "upgrade"

	create = "create"
	kill   = "kill"
	show   = "show"
)

var masterAddr string
var yamlFilename string
var jobId string
var labels string
var instanceId int
var instanceRange string

func main() {

	flag.StringVar(
		&masterAddr,
		"master", "http://localhost:8888", "name of the master address to use (http/tchannel)",
	)

	flag.StringVar(
		&yamlFilename,
		"yaml", "", "name of the yaml file to use",
	)

	flag.StringVar(
		&jobId,
		"jobid", "TestJob_0", "the job id",
	)

	flag.StringVar(
		&labels,
		"labels", "", "the job labels",
	)

	flag.IntVar(
		&instanceId,
		"instanceId", -1, "the task instance id",
	)

	flag.StringVar(
		&instanceRange,
		"range", "", "the task instance range",
	)

	if len(os.Args) < 2 {
		log.Fatalf("Missing args")
	}
	flag.Parse()

	fmt.Println(os.Args)
	fmt.Println("params:", yamlFilename, masterAddr, jobId)
	outbound := http.NewOutbound(masterAddr)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      "peloton-client",
		Outbounds: transport.Outbounds{"peloton-master": outbound},
	})

	if err := dispatcher.Start(); err != nil {
		log.Fatalf("failed to start Dispatcher: %v", err)
	}
	defer dispatcher.Stop()

	client := json.New(dispatcher.Channel("peloton-master"))
	rootCtx := context.Background()
	switch strings.ToLower(os.Args[1]) {
	case Job:
		processJobCommand(rootCtx, client)
	case Task:
		processTaskCommand(rootCtx, client)
	case Upgrade:
		processUpgradeCommand(rootCtx, client)
	default:
		log.Fatalf("Unknown command category, only support: 'job, task, upgrade'")
	}
}

func processJobCommand(ctx context.Context, c json.Client) {
	ctx, _ = context.WithTimeout(ctx, 100*time.Millisecond)
	switch strings.ToLower(os.Args[2]) {
	case "create":
		// Read yaml file to
		var jobConfig job.JobConfig
		buffer, err := ioutil.ReadFile(yamlFilename) // just pass the file name
		if err != nil {
			log.Fatalf("Fail to open file %v, err=%v", yamlFilename, err)
		}
		if err := yaml.Unmarshal(buffer, &jobConfig); err != nil {
			log.Fatalf("Fail to parse file %v, err=%v", yamlFilename, err)
		}
		var response job.CreateResponse
		var request = &job.CreateRequest{
			Id: &job.JobID{
				Value: jobId,
			},
			Config: &jobConfig,
		}
		_, err = c.Call(
			ctx,
			yarpc.NewReqMeta().Procedure("JobManager.Create"),
			request,
			&response,
		)
		log.Infof("Create returns response = %v, err=%v", response, err)
		return
	case "get":
		var response job.GetResponse
		var request = &job.GetRequest{
			Id: &job.JobID{
				Value: jobId,
			},
		}
		_, err := c.Call(
			ctx,
			yarpc.NewReqMeta().Procedure("JobManager.Get"),
			request,
			&response,
		)
		log.Infof("Get job returns response = %v, err=%v", response, err)
		return
	case "query":
	case "delete":
	default:
		log.Fatalf("Unknown command category, only support: 'create, get, query, delete'")
	}
}

func processTaskCommand(ctx context.Context, c json.Client) {
	switch strings.ToLower(os.Args[1]) {
	case "get":
		var request = &task.GetRequest{
			JobId: &job.JobID{
				Value: jobId,
			},
			InstanceId: uint32(instanceId),
		}
		var response task.GetResponse
		_, err := c.Call(
			ctx,
			yarpc.NewReqMeta().Procedure("TaskManager.Get"),
			request,
			&response,
		)
		log.Infof("Get task returns response = %v, err=%v", response, err)
		return
	case "list":
		if len(instanceRange) == 0 {
			log.Fatalf("Missing instance range")
		}
		var from uint32
		var to uint32
		fmt.Sscanf(instanceRange, "%d-%d", from, to)
		var request = &task.ListRequest{
			JobId: &job.JobID{
				Value: jobId,
			},
			Range: &task.InstanceRange{
				From: from,
				To:   to,
			},
		}
		var response task.ListResponse
		_, err := c.Call(
			ctx,
			yarpc.NewReqMeta().Procedure("TaskManager.List"),
			request,
			&response,
		)
		log.Infof("List task returns response = %v, err=%v", response, err)
		return
	case "start":
	case "stop":
	case "restart":
	default:
		log.Fatalf("Unknown command category, only support: 'list, get, start, stop, restart'")
	}
}

func processUpgradeCommand(ctx context.Context, c json.Client) {

}
