#! /usr/bin/env python

import os
import subprocess


class Kind(object):
    """
    KinD is an acronym for K8s in Docker. More documentation about the
    tool can be found here https://github.com/kubernetes-sigs/kind.
    """

    def __init__(self, name):
        self.name = name

    def create(self):
        cmd = ["kind", "create", "cluster", "--name={}".format(self.name)]
        assert subprocess.call(cmd, stderr=subprocess.STDOUT) == 0

    def teardown(self):
        cmd = ["kind", "delete", "cluster", "--name={}".format(self.name)]
        return subprocess.call(cmd, stderr=subprocess.STDOUT) == 0

    def get_port(self):
        cmd = "docker port {}-control-plane 6443/tcp".format(self.name)
        return os.popen(cmd).read().strip()
