.PHONY: all apiserver mock-cqos placement install cli test unit_test cover lint\
clean hostmgr jobmgr resmgr docker version debs docker-push \
	test-containers archiver failure-test-minicluster \
	failure-test-vcluster aurorabridge docs migratedb

.DEFAULT_GOAL := all

PROJECT_ROOT  = github.com/uber/peloton

VENDOR = vendor

# all .go files that don't exist in hidden directories
ALL_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor -e go-build \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*" \
	-e ".*/*.pb.go")
ifndef BIN_DIR
	BIN_DIR = bin
endif
FMT_SRC:=$(shell echo "$(ALL_SRC)" | tr ' ' '\n')
ALL_PKGS = $(shell go list $(sort $(dir $(ALL_SRC))) | grep -v vendor | grep -v mesos-go)

PACKAGE_VERSION=`git describe --always --tags --abbrev=8`
PACKAGE_HASH=`git rev-parse HEAD`
STABLE_RELEASE=`git describe --abbrev=0 --tags`
DOCKER_IMAGE ?= uber/peloton
DC ?= all
GEN_DIR = .gen
#Python gen dir relative to site-packages of python
PYTHON_GEN_DIR = peloton_client/pbgen
UNAME = $(shell uname | tr '[:upper:]' '[:lower:]')

GOKIND = bin/kind
GOCOV = $(go get github.com/axw/gocov/gocov)
GOCOV_XML = $(go get github.com/AlekSi/gocov-xml)
GOLINT = $(go get golang.org/x/lint/golint)
GOIMPORTS = $(go get golang.org/x/tools/cmd/goimports)
GOMOCK = $(go get github.com/golang/mock/gomock github.com/golang/mock/mockgen)
PHAB_COMMENT = .phabricator-comment
# See https://golang.org/doc/gdb for details of the flags
GO_FLAGS = -gcflags '-N -l' -ldflags "-X main.version=$(PACKAGE_VERSION)"

THIS_FILE := $(lastword $(MAKEFILE_LIST))

ifeq ($(shell uname),Linux)
  SED := sed -i -e
else
  SED := sed -i ''
endif

.PRECIOUS: $(GENS) $(LOCAL_MOCKS) $(VENDOR_MOCKS) mockgens

all: gens placement cli hostmgr resmgr jobmgr archiver aurorabridge apiserver \
migratedb mock-cqos

cli:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton cmd/cli/*.go

jobmgr:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-jobmgr cmd/jobmgr/*.go

hostmgr:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-hostmgr cmd/hostmgr/*.go

placement:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-placement cmd/placement/*.go

resmgr:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-resmgr cmd/resmgr/*.go

archiver:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-archiver cmd/archiver/*.go

aurorabridge:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-aurorabridge cmd/aurorabridge/*.go

apiserver:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-apiserver cmd/apiserver/*.go

migratedb:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/migratedb cmd/migratedb/*.go

mock-cqos:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-mock-cqos cmd/mock-cqos/*.go

# Use the same version of mockgen in unit tests as in mock generation
build-mockgen:
	go get ./vendor/github.com/golang/mock/mockgen

$(GOKIND):
	mkdir -p bin
	wget -O $(shell pwd)/bin/kind https://github.com/kubernetes-sigs/kind/releases/download/0.2.1/kind-$(UNAME)-amd64
	chmod a+x $(shell pwd)/bin/kind

# NOTE: `glide install` is flaky, so run it 3 times at most to ensure this doesn't fail
# tests regularly for no reason.
install:
	@if [ -z ${GOPATH} ]; then \
		echo "No $(GOPATH)"; \
		export GOPATH="$(pwd -P)/workspace"; \
		echo "New GOPATH: $(GOPATH)"; \
		mkdir -p "$(GOPATH)/bin"; \
		export GOBIN="$(GOPATH)/bin"; \
		echo "New GOBIN: $(GOBIN)"; \
		export PATH="$(PATH):$(GOBIN)"; \
		echo "New PATH: $(PATH)"; \
	fi
	@if [ ! -d "$(VENDOR)" ]; then \
		echo "Fetching dependencies"; \
		glide --version || go get -u github.com/Masterminds/glide; \
		rm -rf vendor && glide cc && (glide install || glide install || glide install); \
	fi
	@if [ ! -d "env" ]; then \
		which virtualenv || pip install virtualenv ; \
		virtualenv env ; \
		. env/bin/activate ; \
		pip install --upgrade pip ; \
		pip install -r requirements.txt ; \
		deactivate ; \
	fi

$(VENDOR): install

cover:
	./scripts/cover.sh $(shell go list $(PACKAGES))
	go tool cover -html=cover.out -o cover.html

gens: thriftgens pbgens pygens

thriftgens: $(VENDOR)
	@mkdir -p $(GEN_DIR)
	go get ./vendor/go.uber.org/thriftrw
	go get ./vendor/go.uber.org/yarpc/encoding/thrift/thriftrw-plugin-yarpc
	thriftrw --plugin=yarpc --out=$(GEN_DIR)/thrift/aurora pkg/aurorabridge/thrift/api.thrift

pbgens: $(VENDOR)
	@mkdir -p $(GEN_DIR)
	go get ./vendor/github.com/golang/protobuf/protoc-gen-go
	go get ./vendor/go.uber.org/yarpc/encoding/protobuf/protoc-gen-yarpc-go
	./scripts/generate-protobuf.py --generator=go --out-dir=$(GEN_DIR)
    # Temporarily patch the service name in generated rpc code for some v0 APIs
	./scripts/patch-v0-api-rpc.sh
	# Temporarily rename Sla to SLA for lint
	./scripts/rename-job-sla.sh

pygens: $(VENDOR)
	. env/bin/activate; \
	pip install --upgrade pip; \
	pip install grpcio grpcio-tools; \
	./scripts/generate-protobuf.py --generator=python --out-dir=$(PYTHON_GEN_DIR) ;\
    deactivate; \

apidoc: $(VENDOR)
	go get github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc
	./scripts/generate-protobuf.py --generator=doc --out-dir=docs

clean:
	rm -rf vendor gen vendor_mocks $(BIN_DIR) .gen env
	find . -path "*/mocks/*.go" | grep -v "./vendor" | xargs rm -f {}

format fmt: ## Runs "gofmt $(FMT_FLAGS) -w" to reformat all Go files
	@gofmt -s -w $(FMT_SRC)
	./env/bin/autopep8 --exclude "./tools/deploy/aurora/api,./tools/deploy/aurora/schema" -i -r ./tools ./tests ./scripts

comma:= ,
semicolon:= ;

# Helper macro to call mockgen in reflect mode, taking arguments:
# - output directory;
# - package name;
# - semicolon-separated interfaces.
define reflect_mockgen
  mkdir -p $(1) && rm -rf $(1)/*
  mockgen -destination $(1)/mocks.go -self_package mocks -package mocks $(2) $(subst $(semicolon),$(comma),$(3))
  # Fix broken vendor import because of https://github.com/golang/mock/issues/30
  $(SED) s,$(PROJECT_ROOT)/vendor/,, $(1)/mocks.go && goimports -w $(1)/mocks.go
	chmod -R 777 $(1)
endef

# Helper macro to call mockgen in source mode, taking arguments:
# - destination file.
# - source file.
define source_mockgen
  mkdir -p $(dir $(1)) && rm -rf $(dir $(1))*
  mockgen -source $(2) -destination $(1) -self_package mocks -package mocks
  # Fix broken vendor import because of https://github.com/golang/mock/issues/30
  $(SED) s,$(PROJECT_ROOT)/vendor/,, $(1) && goimports -w $(1)
	chmod -R 777 $(dir $(1))
endef


define local_mockgen
  $(call reflect_mockgen,$(1)/mocks,$(PROJECT_ROOT)/$(1),$(2))
endef

define vendor_mockgen
  $(call source_mockgen,vendor_mocks/$(dir $(1))mocks/$(notdir $(1)),vendor/$(1))
endef

mockgens: build-mockgen gens $(GOMOCK)
	$(call local_mockgen,pkg/aurorabridge,RespoolLoader;EventPublisher)
	$(call local_mockgen,pkg/aurorabridge/cache,JobIDCache)
	$(call local_mockgen,pkg/aurorabridge/common,Random)
	$(call local_mockgen,pkg/auth, SecurityManager;SecurityClient;User)
	$(call local_mockgen,pkg/common/concurrency,Mapper)
	$(call local_mockgen,pkg/common/background,Manager)
	$(call local_mockgen,pkg/common/constraints,Evaluator)
	$(call local_mockgen,pkg/common/goalstate,Engine)
	$(call local_mockgen,pkg/common/statemachine,StateMachine)
	$(call local_mockgen,pkg/common/queue,Queue)
	$(call local_mockgen,pkg/common/leader,Candidate;Discovery;Nomination)
	$(call local_mockgen,pkg/middleware/inbound,APILockInterface)
	$(call local_mockgen,pkg/hostmgr,RecoveryHandler)
	$(call local_mockgen,pkg/hostmgr/goalstate,Driver)
	$(call local_mockgen,pkg/hostmgr/host/drainer,Drainer)
	$(call local_mockgen,pkg/hostmgr/hostpool,HostPool)
	$(call local_mockgen,pkg/hostmgr/hostpool/hostmover,HostMover)
	$(call local_mockgen,pkg/hostmgr/hostpool/manager,HostPoolManager)
	$(call local_mockgen,pkg/hostmgr/hostpool/hostmover,HostMover)
	$(call local_mockgen,pkg/hostmgr/mesos,MasterDetector;FrameworkInfoProvider)
	$(call local_mockgen,pkg/hostmgr/offer,EventHandler)
	$(call local_mockgen,pkg/hostmgr/offer/offerpool,Pool)
	$(call local_mockgen,pkg/hostmgr/queue,TaskQueue)
	$(call local_mockgen,pkg/hostmgr/summary,HostSummary)
	$(call local_mockgen,pkg/hostmgr/reconcile,TaskReconciler)
	$(call local_mockgen,pkg/hostmgr/reserver,Reserver)
	$(call local_mockgen,pkg/hostmgr/watchevent,WatchProcessor)
	$(call local_mockgen,pkg/hostmgr/mesos/yarpc/encoding/mpb,SchedulerClient;MasterOperatorClient)
	$(call local_mockgen,pkg/hostmgr/mesos/yarpc/transport/mhttp,Inbound)
	$(call local_mockgen,pkg/hostmgr/p2k/hostcache,HostCache)
	$(call local_mockgen,pkg/hostmgr/p2k/hostcache/hostsummary,HostSummary)
	$(call local_mockgen,pkg/hostmgr/p2k/plugins,Plugin)
	$(call local_mockgen,pkg/jobmgr/cached,JobFactory;Job;Task;JobConfigCache;Update)
	$(call local_mockgen,pkg/jobmgr/goalstate,Driver)
	$(call local_mockgen,pkg/jobmgr/task/activermtask,ActiveRMTasks)
	$(call local_mockgen,pkg/jobmgr/task/lifecyclemgr,Manager;Lockable)
	$(call local_mockgen,pkg/jobmgr/task/event,Listener;StatusProcessor)
	$(call local_mockgen,pkg/jobmgr/logmanager,LogManager)
	$(call local_mockgen,pkg/jobmgr/watchsvc,WatchProcessor)
	$(call local_mockgen,pkg/placement/offers,Service)
	$(call local_mockgen,pkg/placement/hosts,Service)
	$(call local_mockgen,pkg/placement/plugins,Strategy)
	$(call local_mockgen,pkg/placement/tasks,Service)
	$(call local_mockgen,pkg/placement/reserver,Reserver)
	$(call local_mockgen,pkg/placement/models,Offer;Task)
	$(call local_mockgen,pkg/resmgr/respool,ResPool;Tree)
	$(call local_mockgen,pkg/resmgr/preemption,Queue)
	$(call local_mockgen,pkg/resmgr/hostmover,Scorer)
	$(call local_mockgen,pkg/resmgr/queue,Queue;MultiLevelList)
	$(call local_mockgen,pkg/resmgr/task,Scheduler;Tracker)
	$(call local_mockgen,pkg/storage,JobStore;TaskStore;UpdateStore;FrameworkInfoStore;PersistentVolumeStore)
	$(call local_mockgen,pkg/storage/cassandra/api,DataStore)
	$(call local_mockgen,pkg/storage/objects,JobIndexOps;JobNameToIDOps;JobConfigOps;SecretInfoOps;JobRuntimeOps;ResPoolOps;PodEventsOps;JobUpdateEventsOps;ActiveJobsOps;TaskConfigV2Ops;HostInfoOps)
	$(call local_mockgen,pkg/storage/orm,Client;Connector;Iterator)
	$(call local_mockgen,.gen/peloton/api/v0/host/svc,HostServiceYARPCClient)
	$(call local_mockgen,.gen/peloton/api/v0/job,JobManagerYARPCClient)
	$(call local_mockgen,.gen/peloton/api/v0/respool,ResourceManagerYARPCClient)
	$(call local_mockgen,.gen/peloton/api/v0/task,TaskManagerYARPCClient)
	$(call local_mockgen,.gen/peloton/api/v0/update/svc,UpdateServiceYARPCClient)
	$(call local_mockgen,.gen/peloton/api/v0/volume/svc,VolumeServiceYARPCClient)
	$(call local_mockgen,.gen/peloton/api/v1alpha/respool/svc,ResourcePoolServiceYARPCClient)
	$(call local_mockgen,.gen/peloton/api/v1alpha/pod/svc,PodServiceYARPCClient)
	$(call local_mockgen,.gen/peloton/api/v1alpha/job/stateless/svc,JobServiceYARPCClient;JobServiceServiceListJobsYARPCClient;JobServiceServiceListPodsYARPCClient;JobServiceServiceListJobsYARPCServer;JobServiceServiceListPodsYARPCServer)
	$(call local_mockgen,.gen/peloton/api/v1alpha/watch/svc,WatchServiceYARPCClient;WatchServiceServiceWatchYARPCClient;WatchServiceServiceWatchYARPCServer)
	$(call local_mockgen,.gen/qos/v1alpha1,QoSAdvisorServiceYARPCClient)
	$(call local_mockgen,.gen/peloton/api/v1alpha/admin/svc,AdminServiceYARPCClient)
	$(call local_mockgen,.gen/peloton/private/jobmgrsvc,JobManagerServiceYARPCClient)
	$(call local_mockgen,.gen/peloton/private/hostmgr/v1alpha/svc,HostManagerServiceYARPCClient)
	$(call local_mockgen,.gen/peloton/private/hostmgr/hostsvc,InternalHostServiceYARPCClient;InternalHostServiceServiceWatchHostSummaryEventYARPCServer;InternalHostServiceServiceWatchEventStreamEventYARPCServer)
	$(call local_mockgen,.gen/peloton/private/resmgrsvc,ResourceManagerServiceYARPCClient)
	$(call vendor_mockgen,go.uber.org/yarpc/encoding/json/outbound.go)

# launch the test containers to run integration tests and so-on
test-containers:
	bash docker/run_test_cassandra.sh

test: $(GOCOV) gens mockgens test-containers
	gocov test -race $(ALL_PKGS) | gocov report

test_pkg: $(GOCOV) $(GENS) mockgens test-containers
	echo 'Running tests for package $(TEST_PKG)'
	gocov test -race `echo $(ALL_PKGS) | tr ' ' '\n' | grep $(TEST_PKG)` | gocov-html > coverage.html

unit-test: $(GOCOV) $(GENS) mockgens
	gocov test $(ALL_PKGS) --tags "unit" | gocov report

batch-integ-test: $(GOKIND)
	ls -la $(shell pwd)/bin
	PATH="$(PATH):$(shell pwd)/bin" ./tests/run-batch-integration-tests.sh

stateless-integ-test: $(GOKIND)
	ls -la $(shell pwd)/bin
	PATH="$(PATH):$(shell pwd)/bin" ./tests/run-stateless-integration-tests.sh

aurorabridge-integ-test: $(GOKIND)
	ls -la $(shell pwd)/bin
	PATH="$(PATH):$(shell pwd)/bin" ./tests/run-aurorabridge-integration-tests.sh

hostpool-integ-test: $(GOKIND)
	PATH="$(PATH):$(shell pwd)/bin" ./tests/run-hostpool-integration-tests.sh

# launch peloton with PELOTON={any value}, default to none
minicluster: $(GOKIND)
	PATH="$(PATH):$(shell pwd)/bin" PELOTON=$(PELOTON) ./scripts/minicluster.sh setup $(K8S)

minicluster-teardown: $(GOKIND)
	PATH="$(PATH):$(shell pwd)/bin" ./scripts/minicluster.sh teardown

# Clone the newest mimir-lib code. Do not manually edit anything under mimir-lib/*
update-mimir-lib:
	@./scripts/update-mimir-lib.sh

devtools:
	@echo "Installing tools"
	mkdir -p bin
	go get github.com/axw/gocov/gocov
	go get github.com/AlekSi/gocov-xml
	go get github.com/matm/gocov-html
	go get golang.org/x/lint/golint
	go get github.com/golang/mock/gomock
	go get github.com/golang/mock/mockgen
	go get golang.org/x/tools/cmd/goimports
    # temp removing: https://github.com/gemnasium/migrate/issues/26
    # go get github.com/gemnasium/migrate

vcluster:
	rm -rf env ;
	@if [ ! -d "env" ]; then \
		which virtualenv || pip install virtualenv ; \
		virtualenv env ; \
		. env/bin/activate ; \
		pip install --upgrade pip ; \
		pip install -r tools/vcluster/requirements.txt ; \
		deactivate ; \
	fi

version:
	@echo $(PACKAGE_VERSION)

stable-release:
	@echo $(STABLE_RELEASE)

commit-hash:
	@echo $(PACKAGE_HASH)

project-name:
	@echo $(PROJECT_ROOT)

debs:
	@./tools/packaging/build-pkg.sh

# override the built image with IMAGE=
docker:
ifndef IMAGE
	@./tools/packaging/build-docker.sh $(DOCKER_IMAGE):$(PACKAGE_VERSION)
else
	@./tools/packaging/build-docker.sh $(IMAGE)
endif

# override the image to push with IMAGE=
docker-push:
ifndef IMAGE
	@./tools/packaging/docker-push.sh $(REGISTRY) $(DOCKER_IMAGE):$(PACKAGE_VERSION)
else
	@./tools/packaging/docker-push.sh $(REGISTRY) $(IMAGE)
endif

failure-test-minicluster: $(GOKIND)
	IMAGE=uber/peloton $(MAKE) -f $(THIS_FILE) docker
	PATH="$(PATH):$(shell pwd)/bin" ./tests/run-failure-tests.sh minicluster

failure-test-vcluster:
	IMAGE= $(MAKE) -f $(THIS_FILE) docker docker-push
	@./tests/run-failure-tests.sh vcluster

# Jenkins related tasks

LINT_SKIP_ERRORF=grep -v -e "not a string in call to Errorf"
FILTER_LINT := $(if $(LINT_EXCLUDES), grep -v $(foreach file, $(LINT_EXCLUDES),-e $(file)),cat) | $(LINT_SKIP_ERRORF)
# Runs all Go code through "go vet", "golint", and ensures files are formatted using "gofmt"
lint:
	@echo "Running lint"
	@# Skip the last line of the vet output if it contains "exit status"
	@cat /dev/null > vet.log
	@go vet $(ALL_PKGS) 2>&1 | sed '/exit status 1/d' | $(FILTER_LINT) > vet.log || true
	@if [ -s "vet.log" ] ; then \
	    (echo "Go Vet Failures" | cat - vet.log | tee -a $(PHAB_COMMENT) && false) \
	fi;

	@cat /dev/null > vet.log
	@gofmt -e -s -l $(FMT_SRC) | $(FILTER_LINT) > vet.log || true
	@if [ -s "vet.log" ] ; then \
	    (echo "Go Fmt Failures, run 'make fmt'" | cat - vet.log | tee -a $(PHAB_COMMENT) && false) \
	fi;

	@cat /dev/null > vet.log
	./env/bin/autopep8 --exit-code --exclude "./tools/deploy/aurora/api,./tools/deploy/aurora/schema" -d -r ./tools ./tests ./scripts

jenkins: devtools gens mockgens lint
	@chmod -R 777 $(dir $(GEN_DIR)) $(dir $(VENDOR_MOCKS)) $(dir $(LOCAL_MOCKS)) ./vendor_mocks
	go test -race -i $(ALL_PKGS)
	gocov test -v -race $(ALL_PKGS) > coverage.json | sed 's|filename=".*$(PROJECT_ROOT)/|filename="|'
	gocov-xml < coverage.json > coverage.xml


docs:
	@./scripts/mkdocs.sh -q serve
