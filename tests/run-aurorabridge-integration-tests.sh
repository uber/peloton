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

# set up minicluster with SERVICE type for tests under aurorabridge_job/
# Run aurora bridge integration tests in random order
PATH=$PATH:$(pwd)/bin JOB_TYPE=SERVICE pytest -vsrx --count=1 --durations=0 tests/integration/aurorabridge_test  --junit-xml=integration-test-report.xml  -m "$TAGS"

deactivate

popd
