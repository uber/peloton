.PHONY: all master scheduler executor install test cover clean

PBGENDIR = pbgen/src
PROTOC = protoc
PROTOC_FLAGS = --proto_path=protobuf --go_out=$(PBGENDIR)
PBFILES = $(shell find protobuf -name *.proto)
PBGENS = $(PBFILES:%.proto=%.pb.go)

# TODO: figure out why -pkgdir does not work
GOPATH := ${PWD}/pbgen:${GOPATH}

.PRECIOUS: $(PBGENS)

all: $(PBGENS) master scheduler executor

master:
	go build -o ./bin/peloton-master master/main.go

scheduler:
	go build -o ./bin/peloton-scheduler scheduler/main.go

executor:
	go build -o ./bin/peloton-executor executor/main.go

install:
	glide --version || go get github.com/Masterminds/glide
	glide install

test:
	go test $(PACKAGES)


cover:
	./scripts/cover.sh $(shell go list $(PACKAGES))
	go tool cover -html=cover.out -o cover.html

clean:
	rm -rf $(PBGENDIR)/*
	rm bin/*

%.pb.go: %.proto
	${PROTOC} ${PROTOC_FLAGS} $<
