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
extra_flag='-a'
if [ -z "$PELOTON" ] && [ "$1" = "setup" ]; then extra_flag=''; fi
PYTHONPATH=tools/minicluster python tools/minicluster/main.py "$@" $extra_flag

# Exit virtualenv
deactivate
