#! /bin/bash

function peloton::debug () {
    curl -X GET 'http://localhost:5290/logging-level?level=debug&duration=20m'
    curl -X GET 'http://localhost:5291/logging-level?level=debug&duration=20m'
    curl -X GET 'http://localhost:5292/logging-level?level=debug&duration=20m'
    curl -X GET 'http://localhost:5293/logging-level?level=debug&duration=20m'
    curl -X GET 'http://localhost:5303/logging-level?level=debug&duration=20m'
    curl -X GET 'http://localhost:5391/logging-level?level=debug&duration=20m'
}

function peloton::build () {
    GOOS=linux GOARCH=amd64 BIN_DIR=bin-linux make
}

function peloton::setup () {
    [ ! -z "$SKIP_BUILD" ] || peloton::build
    BIND_MOUNTS=$(pwd)/bin-linux:/go/src/github.com/uber/peloton/bin PELOTON=app make minicluster
}

function peloton::respool::create () {
    bin/peloton respool create /DefaultResPool example/default_respool.yaml
}

function peloton::job::stateless::create () {
    bin/peloton job stateless create /DefaultResPool example/stateless/testspec.yaml 0
}

# Example usage:
#     peloton::test:integ tests/integration/batch_job_test/test_job.py
function peloton::test::integ () {
    export BIND_MOUNTS=$(pwd)/bin-linux:/go/src/github.com/uber/peloton/bin
    pytest -p no:random-order -p no:repeat \
        -vsrx --durations=0 $1 \
        --junit-xml=integration-test-report.xml
}
