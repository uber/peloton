BUILD_DIR = .build

# Flags to pass to go build
BUILD_FLAGS =

# Environment variables to set before go build
BUILD_ENV=

# Flags to pass to go test
TEST_FLAGS =

# Extra dependencies that the tests use
TEST_DEPS =

# regex of files which will be removed from test coverage numbers (eg. mock
# object files)
COVER_IGNORE_SRCS = _(mock|string).go

# Exclude the mocks from the linter
LINT_EXCLUDES = .*_mock.go .*_string.go

# Where to find your project
PROJECT_ROOT = code.uber.internal/infra/peloton

# Tells udeploy what your service name is (set to $(notdir of PROJECT_ROOT))
# by default
# SERVICES =

# define the list of thrift files the service depends on
# (if you have some)
THRIFT_SRCS = idl/code.uber.internal/infra/peloton/goal_state.thrift \
	      idl/code.uber.internal/infra/peloton/config_bundle.thrift \

# list all executables
PROGS = master/peloton_master \
	scheduler/peloton_scheduler \
	executor/peloton_executor

master/peloton_master: master/main.go  $(wildcard master/*.go)
scheduler/peloton_scheduler: scheduler/main.go $(wildcard scheduler/*.go)
executor/peloton_executor: executor/main.go $(wildcard executor/*.go)

-include go-build/rules.mk

go-build/rules.mk:
	git submodule update --init
