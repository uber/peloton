#!/usr/bin/env python

import os
import requests
from socket import socket
import time
import yaml

import print_utils

HTTP_LOCALHOST = "http://localhost"

max_retry_attempts = 100
default_host = "localhost"
healthcheck_path = "/health"
sleep_time_secs = 1


#
# Load configs from file
#
def default_config(config='config.yaml', dir=""):
    config_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)) + dir, config
    )
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


#
# Returns whether the zk listening on the given port is ready.
#
def is_zk_ready(port):
    cmd = "bash -c 'echo ruok | nc localhost {}'".format(port)
    return os.popen(cmd).read().strip() == "imok"


#
# Returns a free port on the host.
#
def find_free_port():
    s = socket()
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()
    return port


#
# Goes through the input list and replaces any integer it finds with a random
# free port. If one of the elements is a list itself, we will recursively
# perform this operation in there.
#
def randomize_ports(array):
    for i in range(0, len(array)):
        if type(array[i]) is int:
            array[i] = find_free_port()
        elif type(array[i]) is list:
            array[i] = randomize_ports(array[i])
        else:
            raise Exception("randomize_ports must only be called on lists " +
                            "of lists or integers")
    return array


#
# Run health check for peloton apps
#
def wait_for_up(app, port, path=healthcheck_path):
    count = 0
    error = ""
    url = "http://%s:%s/%s" % (default_host, port, path)
    while count < max_retry_attempts:
        try:
            r = requests.get(url)
            if r.status_code == 200:
                print_utils.okgreen("started %s" % app)
                return
        except Exception as e:
            if count % 5 == 1:
                msg = "app %s is not up yet, retrying at %s" % (app, url)
                print_utils.warn(msg)
            error = str(e)
            time.sleep(sleep_time_secs)
            count += 1

    raise Exception(
        "failed to start %s on %d after %d attempts, err: %s"
        % (app, port, max_retry_attempts, error)
    )
