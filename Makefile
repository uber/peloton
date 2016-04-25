PROJECT_ROOT = code.uber.internal/infra/peloton

# define the list of thrift files the service depends on
# (if you have some)
THRIFT_SRCS = idl/code.uber.internal/infra/peloton/peloton_agent.thrift

# list all executables
PROGS = peloton_agent

peloton_agent: go/agent/main.go go/agent/config.go $(wildcard *.go)

-include go-build/rules.mk

go-build/rules.mk:
	git submodule update --init
