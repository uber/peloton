#!/bin/bash

set -exo pipefail

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

root_dir=$(dirname "$cur_dir")

pushd $root_dir

if [[ -z "${CLUSTER}" ]] && [[ -z "${SKIP_BUILD}" ]]; then
  # TODO: skip build if there is no change
  IMAGE=uber/peloton make docker
fi

virtualenv env

. env/bin/activate

pip install -r tests/integration/requirements.txt

# Allow python path override so we can test any local changes in python client
if [[ -z "${PYTHONPATH}" ]]; then
  PYTHONPATH=$(pwd)
fi

export PYTHONPATH

pytest -vsrx tests/integration

deactivate

popd
