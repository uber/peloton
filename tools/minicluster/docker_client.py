#!/usr/bin/env python

import random
import string
from docker import Client as DockerClient

import print_utils

default_client = DockerClient(base_url="unix://var/run/docker.sock")


class Client(DockerClient):

    def __init__(self, *args, **kwargs):
        self.namespace = kwargs.get('namespace', '')
        kwargs.pop('namespace', None)
        super(Client, self).__init__(*args, **kwargs)

        global default_client
        default_client = self

    def create_container(self, *args, **kwargs):
        name = kwargs.get('name', None)
        if name is None:
            letters = string.ascii_lowercase
            name = ''.join(random.choice(letters) for i in range(10))
        name = self.namespace + name
        kwargs['name'] = name
        return super(Client, self).create_container(*args, **kwargs)

    def containers(self, *args, **kwargs):
        filters = kwargs.get('filters', {})
        if 'name' in filters:
            oldname = filters['name']
            filters['name'] = self.namespace + oldname
        else:
            filters['name'] = '^/{}'.format(self.namespace)
        kwargs['filters'] = filters
        return super(Client, self).containers(*args, **kwargs)

    def exec_create(self, *args, **kwargs):
        kwargs['container'] = self.namespace + kwargs['container']
        return super(Client, self).exec_create(*args, **kwargs)

    # Get container local ip.
    # IP address returned is only reachable on the local machine and within
    # the container.
    def get_container_ip(self, container_name):
        name = u'/{}{}'.format(self.namespace, container_name)
        ctns = super(Client, self).containers()
        if len(ctns) == 0:
            raise Exception("no matching containers found")
        for ctn in ctns:
            if name not in ctn['Names']:
                continue
            networks = ctn['NetworkSettings']['Networks']
            if len(networks) == 0:
                raise Exception("no network settings found for this container")
            for k, v in networks.iteritems():
                if 'IPAddress' in v:
                    return str(v['IPAddress'])
        raise Exception("no ip address found for this container")

    # Force remove container by name (best effort)
    def remove_existing_container(self, container_name):
        container_name = self.namespace + container_name
        try:
            super(Client, self).remove_container(container_name, force=True)
            print_utils.okblue("removed container %s" % container_name)
        except Exception as e:
            if "No such container" in str(e):
                return
            raise e

    # Stop container by name
    def stop_container(self, container_name):
        container_name = self.namespace + container_name
        try:
            super(Client, self).stop(container_name, timeout=5)
            print_utils.okblue("stopped container %s" % container_name)
        except Exception as e:
            if "No such container" in str(e):
                return
            raise e
