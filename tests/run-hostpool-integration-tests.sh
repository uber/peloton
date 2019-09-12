#!/bin/bash

set -exo pipefail

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

root_dir=$(dirname "$cur_dir")

pushd $root_dir

if [[ -z "${CLUSTER}" ]] && [[ -z "${SKIP_BUILD}" ]]; then
  # TODO: skip build if there is no change
  IMAGE=uber/peloton make docker
fi

if [[ ! -d "env" ]]; then
  make install

  . env/bin/activate

  pip install -r tests/requirements.txt

  make pygens
else
  . env/bin/activate
fi

# Allow python path override so we can test any local changes in python client
if [[ -z "${PYTHONPATH}" ]]; then
  PYTHONPATH=$(pwd)
fi

export PYTHONPATH

# enable using host-pools for placement
export USE_HOST_POOL="true"

run_test()
{
  job_type="$1"
  test_path="$2"

  PATH=$PATH:$(pwd)/bin JOB_TYPE=$job_type pytest \
  -p no:random-order \
  -p no:repeat \
  -vsrx \
  --durations=0 \
  $test_path \
  --junit-xml=integration-test-report.xml \
  -m "smoketest or hostpool"
}

# run batch smoke & host-pool tests
run_test BATCH tests/integration/batch_job_test

# run stateless smoke & host-pool tests
run_test SERVICE tests/integration/stateless_job_test

deactivate

popd
