#! /usr/bin/env python

import os
import subprocess
import tempfile
import yaml


class Kind(object):
    """
    KinD is an acronym for K8s in Docker. More documentation about the
    tool can be found here https://github.com/kubernetes-sigs/kind.
    """

    def __init__(self, name, config='tools/minicluster/kind_config.yaml'):
        self.name = name
        self.config = config
        self.kubeconfig = None

    def create(self):
        cmd = ["kind", "create", "cluster", "--name={}".format(self.name)]
        if self.config is not None:
            cmd.append("--config={}".format(self.config))
        try:
            return subprocess.call(cmd) == 0
        except Exception as e:
            print("Failed to create kind cluster: {}".format(e))
            return False

    def teardown(self):
        cmd = ["kind", "delete", "cluster", "--name={}".format(self.name)]
        try:
            return subprocess.call(cmd) == 0
        except Exception as e:
            print("Failed to delete kind cluster: {}".format(e))
            return False

    def get_port(self):
        cmd = "docker port {}-control-plane 6443/tcp".format(self.name)
        return os.popen(cmd).read().strip()

    def is_running(self):
        ctn_name = "{}-control-plane".format(self.name)
        docker_cmd = "docker ps | grep {}".format(ctn_name)
        cmd = ["/bin/bash", "-c", docker_cmd]
        return subprocess.call(cmd) == 0

    def get_kubeconfig(self):
        if self.kubeconfig is not None:
            return self.kubeconfig

        home = os.getenv("HOME")
        config_filename = "kind-config-{}".format(self.name)
        config_location = "{}/.kube/{}".format(home, config_filename)

        # Networking with docker means that localhost doesnt point to
        # the host network, so we need to change the kubeconfig to
        # point to the ip of the container as seen by the
        # other containers.
        new_config = self._rewrite_config(config_location)

        # Create a new directory for the new config, use the same name for
        # the file as the old one.
        dirname = tempfile.mkdtemp(prefix="/tmp/")
        new_config_location = os.path.join(dirname, config_filename)

        with open(new_config_location, 'w') as fh:
            fh.write(new_config)

        print("hacked kubeconfig for osx + docker weirdness: {}".format(
            new_config_location))

        self.kubeconfig = new_config_location
        return new_config_location

    def _rewrite_config(self, prev_config_path):
        with open(prev_config_path, 'r') as fh:
            prev_config = yaml.safe_load(fh)

        ctn_name = "{}-control-plane".format(self.name)
        cmd = "docker inspect --format "
        cmd += "'{{ .NetworkSettings.IPAddress }}' "
        cmd += ctn_name
        ip = os.popen(cmd).read().strip()

        cluster = prev_config["clusters"][0]["cluster"]
        cluster["insecure-skip-tls-verify"] = True
        cluster["server"] = "https://{}:6443".format(ip)
        del cluster["certificate-authority-data"]
        return yaml.dump(prev_config)
