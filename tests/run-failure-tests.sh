#!/bin/bash

function print_help {
  echo "Sets up a Peloton cluster on different platforms and runs failure tests."
  echo "Cleans up the cluster if tests pass."
  echo "Usage: $(basename $0) [options] <driver> [test-options]"
  echo
  echo "Positional parameters"
  echo "  driver      Type of driver to set up Peloton cluster."
  echo "              Supported values: minicluster, vcluster"
  echo
  echo "Options"
  echo "  -h, --help      Display usage"
  echo "  --skip-setup    Do not set up the cluster, just run the tests."
  echo "                  Assumes that the cluster is already set up"
  echo "  --skip-cleanup  Do not clean up the cluster after tests"
  echo "  test-options    These are passed to test-runner (pytest) as is"
  exit 0
}

SKIP_SETUP=0
SKIP_CLEANUP=0
for arg do
   shift
   if [[ $arg == "-h" || $arg == "--help" ]]; then
      print_help
   elif [[ $arg == "--skip-setup" ]];then
      SKIP_SETUP=1
      SKIP_CLEANUP=1
   elif [[ $arg == "--skip-cleanup" ]];then
      SKIP_CLEANUP=1
   else
      set -- "$@" "$arg"
   fi
done

if [[ $# -lt 1 ]]; then
    print_help
fi

export DRIVER=$1
shift

case ${DRIVER} in
   minicluster)
     ;;
   vcluster)
      if [[ -z "${VCLUSTER_ZOOKEEPER}" ]]; then
        echo "Zookeeper location not set in environment variable VCLUSTER_ZOOKEEPER"
        exit 1
      fi
      if [[ -z "${VCLUSTER_PELOTON_IMAGE}" ]]; then
        echo "Docker image for Peloton not set in environment variable VCLUSTER_PELOTON_IMAGE"
        exit 1
      fi
     ;;
   *)
     echo "Unknown driver type ${DRIVER}, see help (-h)"
     exit 1
esac

set -eo pipefail

VCLUSTER_CONFIG_FILE=${VCLUSTER_CONFIG_FILE:-tools/vcluster/config/default-small.yaml}
VCLUSTER_RESPOOL=${VCLUSTER_RESPOOL:-/DefaultResPool}

if [[ -z "${VCLUSTER_LABEL}" ]]; then
  VCLUSTER_LABEL=${USER}_failure_test
fi
VCLUSTER_ARGS="-c ${VCLUSTER_CONFIG_FILE} -z ${VCLUSTER_ZOOKEEPER} -p ${VCLUSTER_RESPOOL} -n ${VCLUSTER_LABEL}"

if [[ -z "${VCLUSTER_AGENT_NUM}" ]]; then
  VCLUSTER_AGENT_NUM="3"
fi
VCLUSTER_SETUP_ARGS="-s ${VCLUSTER_AGENT_NUM} -i ${VCLUSTER_PELOTON_IMAGE} --no-respool --clean"

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
root_dir=$(dirname "$cur_dir")
pushd $root_dir

ENV=failure-env

if [[ ! -d "$ENV" ]]; then
  which virtualenv || pip install virtualenv
  virtualenv $ENV
  . $ENV/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt
  pip install -r tools/vcluster/requirements.txt
  make pygens
  deactivate
fi

. $ENV/bin/activate

# Allow python path override so we can test any local changes in python client
if [[ -z "${PYTHONPATH}" ]]; then
  PYTHONPATH=$(pwd)
fi
export PYTHONPATH
export VCLUSTER_INFO=CONF_${VCLUSTER_LABEL}

if [[ ${SKIP_SETUP} -eq 0 ]]; then
  case ${DRIVER} in
    minicluster)
      echo "Setting up minicluster with Peloton image uber/peloton:latest"
      python tools/minicluster/main.py --num-peloton-instance 2 setup -a
      ;;
    vcluster)
      echo "Setting up vcluster, label ${VCLUSTER_LABEL}, Peloton image ${VCLUSTER_PELOTON_IMAGE}"
      python tools/vcluster/main.py ${VCLUSTER_ARGS} setup ${VCLUSTER_SETUP_ARGS}
      ;;
  esac
fi

if [[ $# -eq 0 ]]; then
  set -- "tests/failure"
fi

pytest -vsrx $@ --junit-xml=failure-test-report.xml

if [[ $? -eq 0 && ${SKIP_CLEANUP} -eq 0 ]]; then
  case ${DRIVER} in
    minicluster)
      echo "Tearing down minicluster"
      python tools/minicluster/main.py --num-peloton-instance 2 teardown
      ;;
    vcluster)
      echo "Tearing down vcluster, label ${VCLUSTER_LABEL}"
      python tools/vcluster/main.py ${VCLUSTER_ARGS} teardown --remove
      ;;
  esac
else
  echo "Skipped tearing down Peloton cluster"
fi

deactivate

popd
