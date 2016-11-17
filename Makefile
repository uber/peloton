SHELL = /bin/bash -o pipefail

PROJECT_ROOT  = code.uber.internal/infra/peloton

GO            := go
GOFMT         := gofmt

GLIDE_YAML := $(CURDIR)/glide.yaml
GLIDE_LOCK := $(CURDIR)/glide.lock

SERVICES ?= $(subst .git,,$(notdir $(PROJECT_ROOT)))

BUILD_DIR ?= .tmp

# verbosity rules
V ?= 0
ifeq ($(V),0)
  ECHO_V = @
else
  TEST_VERBOSITY_FLAG = -v
endif

# _GOPATH is the new GOPATH for the project
_GOPATH = $(CURDIR)/$(BUILD_DIR)/.goroot

# override Go environment with our local one
OLDGOPATH := $(GOPATH)

# you can't have Make depend on a directory as the tstamp is updated
# when a file is written to it.  Instead, touch a file that says you
# created the directory
FAUXFILE := $(BUILD_DIR)/.faux
FAUXROOT := $(_GOPATH)/src/$(PROJECT_ROOT)

# Test for a broken root symlink, which can happen when syncing projects
# across machines/VMs. -h tests for a symlink, ! -e means the file does not exist.
ifeq ($(shell [ -h $(FAUXROOT) ] && [ ! -e $(FAUXROOT) ] && echo "b"),b)
$(warning Broken root detecting, cleaning symlink)
$(shell unlink $(FAUXROOT))
$(shell rm $(FAUXFILE))
endif

# Sentinel file to detect whether dependencies are installed
FAUX_VENDOR := $(BUILD_DIR)/.faux.vendor

USE_VENDOR := y
export GO15VENDOREXPERIMENT=1

# so people in their makefiles can depend on vendor
$(VENDOR_DIR): $(FAUX_VENDOR)

$(FAUX_VENDOR): $(FAUXFILE) $(GLIDE)
	@[ ! -f $(GLIDE_YAML) ] || [ -f $(GLIDE_LOCK) ] || (echo "$(GLIDE_YAML) present, but missing $(GLIDE_LOCK) file. Make sure it's checked in." && exit 1)
	@# Retry the glide install a few times to avoid flaky Jenkins failures
	[ ! -f $(GLIDE_YAML) ] || (cd $(FAUXROOT) && \
	    for i in 1 2 3; do \
		    glide install && break; \
	    done)
	@touch $@

# FAUXTEST is used as a dependency for tests to ensure that we build
# dependencies once (using go test -i) before trying to run each test.
# Otherwise, multiple go test invocations will not save any build output
# and dependencies are recompiled multiple times.
FAUXTEST := $(BUILD_DIR)/faux.test

# Set this flag to 0 in your Makefile if you're using ginkgo, whose test log output
# will cause the go-junit-reporter to think your tests are failed (since they don't conform
# to the stdlib testing stuff like testify/assert does)
ENABLE_JUNIT_XML ?= 1

TEST_FLAGS += $(RACE) $(TEST_VERBOSITY_FLAG)
TEST_ENV ?= UBER_ENVIRONMENT=test UBER_CONFIG_DIR=$(abspath $(FAUXROOT))/config/master

TEST_DEPS ?=

# mock files will be removed from test coverage reports
COVER_IGNORE_SRCS ?= _mock\.go _string\.go mocks/.*\.go mock_.*\.go

# Test coverage target files
PHAB_COMMENT ?= /dev/null
HTML_REPORT := coverage.html
COVERAGE_XML := coverage.xml
JUNIT_XML := junit.xml
COVERFILE := $(BUILD_DIR)/cover.out
TEST_LOG := $(BUILD_DIR)/test.log
LINT_LOG := $(BUILD_DIR)/lint.log
FMT_LOG := $(BUILD_DIR)/fmt.log
VET_LOG := $(BUILD_DIR)/vet.log

# all .go files that don't exist in hidden directories
ALL_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor -e go-build -e mesos-go -e pbgen \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*")

# !!!IMPORTANT!!!  This must be = and not := as this rule must be delayed until
# after FAUXFILE has been executed.  Only use this in rules that depend on
# FAUXFILE!
ALL_PKGS = $(shell (cd $(FAUXROOT); \
	   GOPATH=$(GOPATH) $(GO) list $(sort $(dir $(ALL_SRC)))) | grep -v mesos-go)

# all directories with *_test.go files in them
TEST_DIRS := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))

# profile.cov targets for test coverage
TEST_PROFILE_TGTS := $(addprefix $(BUILD_DIR)/, $(addsuffix profile.cov, $(TEST_DIRS)))
TEST_PROFILE_TMP_TGTS := $(addsuffix .tmp,$(TEST_PROFILE_TGTS))
TEST_TMP_LOGS := $(addprefix $(BUILD_DIR)/, $(addsuffix test.log.tmp, $(TEST_DIRS)))

$(TEST_LOG): $(FAUXFILE) $(TEST_PROFILE_TMP_TGTS)
	@cat /dev/null $(TEST_TMP_LOGS) > $(TEST_LOG)

$(COVERFILE): $(FAUXFILE) $(TEST_PROFILE_TGTS)
	@echo "mode: count" | cat - $(TEST_PROFILE_TGTS) > $(COVERFILE)
	if [ -n "${COVER_IGNORE_SRCS}" ]; then sed -i\"\" -E "$(foreach pattern, $(COVER_IGNORE_SRCS),\#$(pattern)#d; )" $(COVERFILE); fi

$(FAUXTEST): $(FAUXFILE) $(ALL_SRC) $(TEST_DEPS)
	@touch $@

$(BUILD_DIR)/%/profile.cov: $(BUILD_DIR)/%/profile.cov.tmp
	@([ -f $< ] && tail -n +2 $< > $@) || (echo "Test $1 failed" && false)

$(BUILD_DIR)/%/profile.cov.tmp: $(ALL_SRC) $(TEST_DEPS) $(FAUXTEST)
	@mkdir -p $(dir $@)
	@touch $@
	$(ECHO_V) cd $(FAUXROOT); $(TEST_ENV)	\
		$(GO) test $(TEST_FLAGS) -coverprofile=$@ $*/  | \
		tee $(BUILD_DIR)/$*/test.log.tmp || \
			(cat $(BUILD_DIR)/$*/test.log.tmp >> $(PHAB_COMMENT) && rm -rf $@)

ifeq ($(ENABLE_JUNIT_XML),1)

$(JUNIT_XML): $(TEST_LOG)
	go-junit-report < $(TEST_LOG) > $@

else

.PHONY: $(JUNIT_XML)
$(JUNIT_XML):
	@echo JUnit reporting disabled by ENABLE_JUNIT_XML=$(ENABLE_JUNIT_XML) flag

endif

$(COVERAGE_XML): $(COVERFILE)
	gocov convert $(COVERFILE) | gocov-xml | sed 's|filename=".*$(PROJECT_ROOT)/|filename="|' > $@

$(HTML_REPORT): $(COVERFILE)
	gocov convert $(COVERFILE) | gocov-html > $@

$(FAUXFILE):
	@mkdir -p $(_GOPATH)/src/$(dir $(PROJECT_ROOT))
	@ln -s $(CURDIR) $(_GOPATH)/src/$(PROJECT_ROOT)
	@touch $(FAUXFILE)

GOPATH := $(FAUXROOT)/pbgen:$(_GOPATH)
export GOPATH

# Prefer tools from $GOPATH/bin over those elsewhere on the path.
export PATH := $(PATH):$(GOPATH)/bin

# All of the following are PHONY targets for convenience.

devtools:
	@echo "Installing tools"
	GOPATH=$(_GOPATH) && go get github.com/axw/gocov/gocov
	GOPATH=$(_GOPATH) && go get github.com/AlekSi/gocov-xml
	GOPATH=$(_GOPATH) && go get github.com/matm/gocov-html
	GOPATH=$(_GOPATH) && go get github.com/golang/lint/golint
	GOPATH=$(_GOPATH) && go get github.com/Masterminds/glide
	GOPATH=$(_GOPATH) && go get github.com/jstemmer/go-junit-report
	GOPATH=$(_GOPATH) && go get github.com/golang/protobuf/{proto,protoc-gen-go}

bins: $(PROGS) ## Build all binaries specified in the PROGS variable.

test:: $(COVERFILE) $(TEST_LOG) ## Runs tests for all subpackages.
	gocov convert $(COVERFILE) | gocov report

test-xml: $(COVERAGE_XML) $(JUNIT_XML)

test-html: $(HTML_REPORT)

jenkins:: clean devtools install-proto proto test-mysql ## Helper rule for jenkins, which calls clean, then runs tests, and then runs lint.
	@# run this first because it can't be parallelized
	$(MAKE) $(FAUXFILE) $(FAUX_VENDOR)
	$(MAKE) $(JENKINS_PARALLEL_FLAG) -k \
		test-xml \
		RACE="-race" \
		TEST_VERBOSITY_FLAG="-v" \
		PHAB_COMMENT=.phabricator-comment
	@# lint needs installed packages to avoid spurious errors
	$(MAKE) lint PHAB_COMMENT=.phabricator-comment

testhtml: test-html

testxml: test-xml

service-names:
	@echo $(SERVICES)

clean: ## Removes all build objects, including binaries, generated code, and test output files
	@rm -rf $(PROGS) $(BUILD_DIR) $(BIN_DIR) protoc *.html *.xml .phabricator-comment *.zip*

# Create a pipeline filter for go vet/golint. Patterns specified in LINT_EXCLUDES are
# converted to a grep -v pipeline. If there are no filters, cat is used.
FILTER_LINT := $(if $(LINT_EXCLUDES), grep -v $(foreach file, $(LINT_EXCLUDES),-e $(file)),cat)

lint: $(GOLINT) $(FAUXFILE) ## Runs all Go code through "go vet", "golint", and ensures files are formatted using "gofmt"
	@# Skip the last line of the vet output if it contains "exit status"
	@cd $(FAUXROOT); $(GO) vet $(ALL_PKGS) 2>&1 | sed '/exit status 1/d' | $(FILTER_LINT) > $(VET_LOG) || true
	@[ ! -s "$(VET_LOG)" ] || (echo "Go Vet Failures" | cat - $(VET_LOG) | tee -a $(PHAB_COMMENT) && false)
	@cat /dev/null > $(LINT_LOG)
	@cd $(FAUXROOT); $(foreach pkg, $(ALL_PKGS), \
		$(GOLINT) $(pkg) | $(FILTER_LINT) >> $(LINT_LOG) || true;)
	@[ ! -s "$(LINT_LOG)" ] || (echo "Lint Failures" | cat - $(LINT_LOG) | tee -a $(PHAB_COMMENT) && false)
	@$(GOFMT) -e -s -l $(ALL_SRC) | $(FILTER_LINT) > $(FMT_LOG) || true
	@[ ! -s "$(FMT_LOG)" ] || (echo "Go Fmt Failures, run 'make fmt'" | cat - $(FMT_LOG) | tee -a $(PHAB_COMMENT) && false)

# We only want to format the files that are not ignored by FILTER_LINT.
FMT_SRC:=$(shell echo "$(ALL_SRC)" | tr ' ' '\n' | $(FILTER_LINT))
fmt: ## Runs "gofmt $(FMT_FLAGS) -w" to reformat all Go files
	gofmt $(FMT_FLAGS) -w $(FMT_SRC)



# Protoc related
PROTOC_VERSION=3.0.2
PBGEN_DIR = pbgen/src
PROTOC = protoc
PROTOC_FLAGS = --proto_path=protobuf --go_out=$(PBGEN_DIR)
PBFILES = $(shell find protobuf -name *.proto)
PBGENS = $(PBFILES:%.proto=%.pb.go)

# for downloading the correct protoc
UNAME := $(shell uname)
ifeq ($(UNAME),Linux)
    OS_detected := linux
else ifeq ($(UNAME),Darwin)
    OS_detected := osx
endif

install-proto:
	@echo "Installing protoc"
	if [ ! -d "./protoc" ]; then \
		wget https://github.com/google/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(OS_detected)-x86_64.zip -o /dev/null ; \
		unzip -d protoc protoc-$(PROTOC_VERSION)-$(OS_detected)-x86_64.zip ; \
	fi

# Prefer protoc from protoc/bin instaed of elsewhere in the PATH
export PATH := $(PWD)/protoc/bin:$(PATH)
proto: $(PBGENS) ## generates go-lang bindings from protobuf

%.pb.go: %.proto
	@mkdir -p $(PBGEN_DIR)
	${PROTOC} ${PROTOC_FLAGS} $<

# default make objective
all: $(PBGENS) master scheduler executor client

# start mysql container
test-mysql:
	bash docker/run_test_mysql.sh

# Binaries
GO_FLAGS = -gcflags '-N'
BIN_DIR = bin

master: $(FAUXFILE)
	@mkdir -p $(BIN_DIR)
	GOPATH=$(OLDGOPATH):$(PWD)/pbgen $(GO) build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-master master/*.go

scheduler: $(FAUXFILE)
	GOPATH=$(OLDGOPATH):$(PWD)/pbgen $(GO) build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-scheduler scheduler/main/main.go

executor: $(FAUXFILE)
	GOPATH=$(OLDGOPATH):$(PWD)/pbgen $(GO) build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-executor executor/main.go

client: $(FAUXFILE)
	GOPATH=$(OLDGOPATH):$(PWD)/pbgen $(GO) build $(GO_FLAGS) -o ./$(BIN_DIR)/peloton-client cli/peloton-client.go

bootstrap-dev:
	cd docker && bash bootstrap.sh

help: ## Prints a help message that shows rules and any comments for rules.
	@cat $(MAKEFILE_LIST) | grep -e "^[a-zA-Z_\-]*: *.*## *" | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Useful Variables:"
	@echo "  V    enable a more verbose build, when you want to see how the sausage is made."
	@echo
	@echo "Among other methods, you can pass these variables on the command line, e.g.:"
	@echo "    make V=1"



.PRECIOUS: $(PBGENS)
.DEFAULT_GOAL = all
.PHONY: \
	all \
	bins \
	clean \
	fmt \
	help \
	install-all \
	jenkins \
	lint \
	service-names \
	test \
	test-html \
	testhtml \
	test-xml \
	testxml \
	proto \
	master \
	scheduler \
	executor \
	client \
