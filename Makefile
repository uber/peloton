.PHONY: all placement executor install client test unit_test cover lint clean hostmgr jobmgr resmgr docker version debs docker-push test-containers db-pressure
.DEFAULT_GOAL := all

PROJECT_ROOT  = code.uber.internal/infra/peloton

# all .go files that don't exist in hidden directories
ALL_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor -e go-build \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*" \
	-e ".*/*.pb.go")
BIN_DIR = bin
FMT_SRC:=$(shell echo "$(ALL_SRC)" | tr ' ' '\n')
ALL_PKGS = $(shell go list $(sort $(dir $(ALL_SRC))) | grep -v vendor | grep -v mesos-go)

PACKAGE_VERSION=`git describe --always --tags`
DOCKER_IMAGE ?= uber/peloton
DC ?= all
PBGEN_DIR = .gen

GOCOV = $(go get github.com/axw/gocov/gocov)
GOCOV_XML = $(go get github.com/AlekSi/gocov-xml)
GOLINT = $(go get github.com/golang/lint/golint)
GOIMPORTS = $(go get golang.org/x/tools/cmd/goimports)
GOMOCK = $(go get github.com/golang/mock/gomock github.com/golang/mock/mockgen)
PHAB_COMMENT = .phabricator-comment
PACKAGE_VERSION=`git describe --always --tags`
# See https://golang.org/doc/gdb for details of the flags
GO_FLAGS = -gcflags '-N -l' -ldflags "-X main.version=$(PACKAGE_VERSION)"

ifeq ($(shell uname),Linux)
  SED := sed -i -e
else
  SED := sed -i ''
endif

.PRECIOUS: $(PBGENS) $(LOCAL_MOCKS) $(VENDOR_MOCKS) mockgens

all: pbgens placement executor client hostmgr resmgr jobmgr

jobmgr:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-jobmgr jobmgr/main/*.go

hostmgr:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-hostmgr hostmgr/main/*.go

placement:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-placement placement/main/*.go

resmgr:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-resmgr resmgr/main/*.go

executor:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-executor executor/*.go

db-pressure:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/dbpressure storage/pressuretest/main/*.go

install:
	glide --version || go get github.com/Masterminds/glide
	rm -rf vendor && glide install

client:
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton client/cli/*.go

cover:
	./scripts/cover.sh $(shell go list $(PACKAGES))
	go tool cover -html=cover.out -o cover.html

pbgens:
	@mkdir -p $(PBGEN_DIR)
	./scripts/generate-protobuf.py

clean:
	rm -rf vendor/mesos vendor/peloton pbgen
	rm -rf vendor_mocks
	find . -path "*/mocks/*.go" | grep -v "./vendor" | xargs rm -f {}
	rm -rf $(BIN_DIR)
	rm -rf .gen

format fmt: ## Runs "gofmt $(FMT_FLAGS) -w" to reformat all Go files
	@gofmt -s -w $(FMT_SRC)

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

mockgens: pbgens $(GOMOCK)
	$(call local_mockgen,hostmgr/mesos,MasterDetector;FrameworkInfoProvider)
	$(call local_mockgen,hostmgr/offer,EventHandler)
	$(call local_mockgen,hostmgr/reconcile,TaskReconciler)
	$(call local_mockgen,storage,JobStore;TaskStore;FrameworkInfoStore;ResourcePoolStore)
	$(call local_mockgen,yarpc/encoding/mpb,SchedulerClient;MasterOperatorClient)
	$(call local_mockgen,yarpc/transport/mhttp,Inbound)
	$(call vendor_mockgen,go.uber.org/yarpc/encoding/json/outbound.go)

# launch the test containers to run integration tests and so-on
test-containers:
	bash docker/run_test_mysql.sh
	bash docker/run_test_cassandra.sh

test: $(GOCOV) pbgens mockgens test-containers
	gocov test -race $(ALL_PKGS) | gocov report

unit-test: $(GOCOV) $(PBGENS) mockgens
	gocov test $(ALL_PKGS) --tags "unit" | gocov report

integ-test:
	@./tests/run-integration-tests.sh

# launch peloton with PELOTON={app}, default to none
# TODO: clean up the make rule when cleaning up pcluster
pcluster:
# installaltion of docker-py is required, see "bootstrap.sh" or ""tools/pcluster/README.md" for more info
ifndef PELOTON
	@./tools/pcluster/pcluster.py setup
else
ifeq ($(PELOTON),app)
	@./tools/pcluster/pcluster.py setup -a
else
	@echo "Unknown Peloton mode: "$(PELOTON)
endif
endif

pcluster-teardown:
	@./tools/pcluster/pcluster.py teardown

devtools:
	@echo "Installing tools"
	go get github.com/axw/gocov/gocov
	go get github.com/AlekSi/gocov-xml
	go get github.com/matm/gocov-html
	go get github.com/golang/lint/golint
	go get github.com/golang/mock/gomock
	go get github.com/golang/mock/mockgen
	go get golang.org/x/tools/cmd/goimports

version:
	@echo $(PACKAGE_VERSION)

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
	@./tools/packaging/docker-push.sh $(DOCKER_IMAGE):$(PACKAGE_VERSION)
else
	@./tools/packaging/docker-push.sh $(IMAGE)
endif

# Jenkins related tasks

LINT_SKIP_ERRORF=grep -v -e "not a string in call to Errorf"
FILTER_LINT := $(if $(LINT_EXCLUDES), grep -v $(foreach file, $(LINT_EXCLUDES),-e $(file)),cat) | $(LINT_SKIP_ERRORF)
# Runs all Go code through "go vet", "golint", and ensures files are formatted using "gofmt"
lint: devtools
	@echo "Running lint"
	@# Skip the last line of the vet output if it contains "exit status"
	@go vet $(ALL_PKGS) 2>&1 | sed '/exit status 1/d' | $(FILTER_LINT) > vet.log || true
	@if [ -s "vet.log" ] ; then \
	    (echo "Go Vet Failures" | cat - vet.log | tee -a $(PHAB_COMMENT) && false) \
	fi;

	@cat /dev/null > vet.log
	@gofmt -e -s -l $(FMT_SRC) | $(FILTER_LINT) > vet.log || true
	@if [ -s "vet.log" ] ; then \
	    (echo "Go Fmt Failures, run 'make fmt'" | cat - vet.log | tee -a $(PHAB_COMMENT) && false) \
	fi;

jenkins: devtools lint pbgens mockgens
	@chmod -R 777 $(dir $(PBGEN_DIR)) $(dir $(VENDOR_MOCKS)) $(dir $(LOCAL_MOCKS)) ./vendor_mocks
	gocov test -v -race $(ALL_PKGS) > coverage.json | sed 's|filename=".*$(PROJECT_ROOT)/|filename="|'
	gocov-xml < coverage.json > coverage.xml
