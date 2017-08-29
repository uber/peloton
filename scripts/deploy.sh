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
pip install -q -r tools/deploy/requirements.txt

# Run the deployment script 
PYTHONPATH=tools/deploy; python tools/deploy/main.py "$@"

# Exit virtualenv
deactivate
