.PHONY: all master scheduler executor install client test cover clean

# all .go files that don't exist in hidden directories
ALL_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor -e go-build \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*" \
	-e ".*/*.pb.go")
BIN_DIR = bin
FMT_SRC:=$(shell echo "$(ALL_SRC)" | tr ' ' '\n')
ALL_PKGS = $(shell go list $(sort $(dir $(ALL_SRC))) | grep -v vendor | grep -v mesos-go)
PBGEN_DIR = pbgen/src
PROTOC = protoc
PROTOC_FLAGS = --proto_path=protobuf --go_out=$(PBGEN_DIR)
PBFILES = $(shell find protobuf -name *.proto)
PBGENS = $(PBFILES:%.proto=%.pb.go)
GOCOV = $(go get github.com/axw/gocov/gocov)
GO_FLAGS = -gcflags '-N'
# TODO: figure out why -pkgdir does not work
GOPATH := ${PWD}/pbgen:${GOPATH}
.PRECIOUS: $(PBGENS)

all: $(PBGENS) master scheduler executor client

master:
	@mkdir -p $(BIN_DIR)
	go build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-master master/*.go

scheduler:
	go build -o ./$(BIN_DIR)/peloton-scheduler scheduler/main/main.go

executor:
	go build -o ./$(BIN_DIR)/peloton-executor executor/main.go

install:
	glide --version || go get github.com/Masterminds/glide
	glide install

client:
	go build -o ./$(BIN_DIR)/peloton-client cli/peloton-client.go

cover:
	./scripts/cover.sh $(shell go list $(PACKAGES))
	go tool cover -html=cover.out -o cover.html

clean:
	rm -rf pbgen
	rm -rf $(BIN_DIR)

format fmt: ## Runs "gofmt $(FMT_FLAGS) -w" to reformat all Go files
	gofmt -w $(FMT_SRC)

test: $(GOCOV)
	gocov test $(ALL_PKGS) | gocov report

# MYSQL should be run against mysql with port 8193, which can be launched in container by running docker/bootstrap.sh
MYSQL = mysql --host=127.0.0.1 -P 8193
MYSQL_PELOTON = $(MYSQL) -upeloton -ppeloton

bootstrap:
	@echo Creating database
	$(MYSQL_PELOTON) -e 'create database if not exists peloton'


%.pb.go: %.proto
	@mkdir -p $(PBGEN_DIR)
	${PROTOC} ${PROTOC_FLAGS} $<
