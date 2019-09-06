#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

# Setup virtualenv if not exists
if [ ! -d env ]; then
    virtualenv env
fi

# Active virtualenv
. env/bin/activate

# Install pip requirements for the deployment script
pip install -q -r tools/minicluster/requirements.txt

# Run the deployment script
extra_flag=''
if [ -n "$PELOTON" ] && [ "$1" = "setup" ]; then extra_flag='-a'; fi
if [[ -n "$USE_HOST_POOL" ]]; then extra_flag="$extra_flag --use-host-pool"; fi
PYTHONPATH=tools/minicluster python tools/minicluster/main.py "$@" $extra_flag

# Exit virtualenv
deactivate
