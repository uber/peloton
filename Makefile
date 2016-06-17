.PHONY: all master scheduler executor install test cover clean

BIN_DIR = bin
PBGEN_DIR = pbgen/src
PROTOC = protoc
PROTOC_FLAGS = --proto_path=protobuf --go_out=$(PBGEN_DIR)
PBFILES = $(shell find protobuf -name *.proto)
PBGENS = $(PBFILES:%.proto=%.pb.go)

# TODO: figure out why -pkgdir does not work
GOPATH := ${PWD}/pbgen:${GOPATH}

.PRECIOUS: $(PBGENS)

all: $(PBGENS) master scheduler executor

master:
	@mkdir -p $(BIN_DIR)
	go build -o ./$(BIN_DIR)/peloton-master master/main.go

scheduler:
	go build -o ./$(BIN_DIR)/peloton-scheduler scheduler/main.go

executor:
	go build -o ./$(BIN_DIR)/peloton-executor executor/main.go

install:
	glide --version || go get github.com/Masterminds/glide
	glide install

test:
	go test $(PACKAGES)


cover:
	./scripts/cover.sh $(shell go list $(PACKAGES))
	go tool cover -html=cover.out -o cover.html

clean:
	rm -rf pbgen
	rm -rf $(BIN_DIR)

%.pb.go: %.proto
	@mkdir -p $(PBGEN_DIR)
	${PROTOC} ${PROTOC_FLAGS} $<
