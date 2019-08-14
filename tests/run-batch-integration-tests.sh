#!/bin/bash

set -exo pipefail

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

root_dir=$(dirname "$cur_dir")

pushd $root_dir

if [[ -z "${CLUSTER}" ]] && [[ -z "${SKIP_BUILD}" ]]; then
  # TODO: skip build if there is no change
  IMAGE=uber/peloton make docker
fi

make install

. env/bin/activate

pip install -r tests/requirements.txt

make pygens

# Allow python path override so we can test any local changes in python client
if [[ -z "${PYTHONPATH}" ]]; then
  PYTHONPATH=$(pwd)
fi

export PYTHONPATH

if [[ -z "${TAGS}" ]]; then
  TAGS='default'
fi

# If TAGS is not set, all tests from default group will run
# set up minicluster with BATCH type for tests under batch_job_test/
PATH=$PATH:$(pwd)/bin JOB_TYPE=BATCH pytest -p no:random-order -p no:repeat -vsrx --durations=0 tests/integration/batch_job_test --junit-xml=integration-test-report.xml -m "$TAGS"

# set up minicluster with BATCH type for tests under misc_tes/
PATH=$PATH:$(pwd)/bin JOB_TYPE=BATCH pytest -p no:random-order -p no:repeat -vsrx --durations=0 tests/integration/misc_test --junit-xml=integration-test-report.xml -m "$TAGS"

deactivate

popd
