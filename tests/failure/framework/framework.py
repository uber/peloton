import json
import logging
import os
import re

from minicluster_driver import MiniClusterDriver
import tools.minicluster as minicluster
from vcluster_driver import VClusterDriver

log = logging.getLogger(__name__)


class FailureFramework(object):
    """
    A framework for inducing failures on different components
    of a Peloton cluster. Works with Peloton deployed on
    a variety of situations, such as local development cluster
    (minicluster), virtualized Peloton (vcluster) etc.
    The framework mainly provides methods to start and stop
    component instances. These methods allow selecting the
    instances to operate on in the following ways:
    - Specifying an absolute number, e.g. "5"
    - Specifying a percentage, e.g. "50%"
    - Specifying a role, e.g. "leader"
    """

    def __init__(self):
        self._client = None

    def setup(self, *args, **kwargs):
        """
        Initialize the framework for use. The driver is to use
        is determined from "driver" keyword argument. If that is not
        provided, environment variable DRIVER is used. The default
        driver is minicluster.
        :param args: Positional arguments for driver setup. These
        arguments are driver specific and passed on the driver
        unchanged; see a particular driver for details.
        :param kwargs: Keyword arguments for driver setup. These
        arguments are driver specific and passed on the driver
        unchanged; see a particular driver for details.
        """
        kind = os.environ.get("DRIVER", "minicluster")
        kind = kwargs.get("driver", kind)
        if kind == "minicluster":
            docker_client = minicluster.docker_client.default_client
            self.driver = MiniClusterDriver(docker_client)
        elif kind == "vcluster":
            self.driver = VClusterDriver()
        else:
            raise Exception("Unknown driver %s" % kind)
        log.info("Using driver %s", kind)
        self.driver.setup(args, kwargs)
        info = self.driver.info()
        for item in info:
            log.info("%s: %s: %s", kind, item[0], item[1])

    def teardown(self, *args, **kwargs):
        """
        Cleans up framework state.
        :param args: Positional arguments for driver teardown. These
        arguments are driver specific and passed on the driver
        unchanged; see a particular driver for details.
        :param kwargs: Keyword arguments for driver teardown. These
        arguments are driver specific and passed on the driver
        unchanged; see a particular driver for details.
        """
        self.driver.teardown(args, kwargs)

    @property
    def client(self):
        """
        Get a PelotonClient instance for communicating with the Peloton cluster
        under test. Caches the client for future use.
        """
        if not self._client:
            self._client = self.driver.get_peloton_client("failure-test")
        return self._client

    def reset_client(self):
        """
        Clear cached PelotonClient.
        """
        self._client.discovery.stop()
        self._client = None

    def stop(self, component, selector=None):
        """
        Stop instances of a component that match selector. If stopping an
        instance fails, the exception is raised back to the caller
        immediately. Stopping an already stopped instance does not raise an
        exception.
        :param component: Component to stop
        :param selector: String. Instance selection criteria. See class
        level comments for supported values.
        :return: Number of instances that were stopped
        """
        cts = self.driver.find(component.name, running_only=True)
        ct_ids = self._select_instances(component, cts, selector)
        for ct in ct_ids:
            log.info("Stopping component %s: instance %s", component.name, ct)
            self.driver.stop(ct)
        return len(cts)

    def start(self, component, selector=None):
        """
        Start instances of a component that match selector. If starting an
        instance fails, the exception is raised back to the caller
        immediately. Starting an already started instance does not raise an
        exception.
        :param component: Component to start
        :param selector: String. Instance selection criteria. See class
        level comments for supported values.
        :return: Number of instances that were started
        """
        cts = self.driver.find(component.name, running_only=False)
        ct_ids = self._select_instances(component, cts, selector)
        for ct in ct_ids:
            log.info("Starting component %s: instance %s", component.name, ct)
            self.driver.start(ct)
        return len(cts)

    def restart(self, component, selector=None):
        """
        Restart instances of a component that match selector. If restarting an
        instance fails, the exception is raised back to the caller
        immediately. Instances may be left in an unpredictable state if
        that happens (started or stopped). Callers can then use the start and
        stop methods to bring the instances to a desired state.
        :param component: Component to restart
        :param selector: String. Instance selection criteria. See class
        level comments for supported values.
        :return: Number of instances that were restarted
        """
        cts = self.driver.find(component.name, running_only=False)
        ct_ids = self._select_instances(component, cts, selector)
        for ct in ct_ids:
            log.info(
                "Restarting component %s: instance %s", component.name, ct
            )
            self.driver.stop(ct)
            self.driver.start(ct)
        return len(cts)

    def get_leader_info(self, component):
        """
        Get Zookeeper data for a component leader.
        :param component: Component to get leader info for
        :return: Dict with leader data
        """
        if not component.zk_leader_path:
            assert False
        zknode, _ = self.client.discovery.zk.get(component.zk_leader_path)
        return json.loads(zknode)

    def _select_instances(self, component, insts, selector):
        if not selector or not insts:
            return insts.keys() if insts else []
        selector = selector.strip()
        count = 0
        if selector == "leader":
            zk = self.get_leader_info(component)
            if zk:
                for cid, cinfo in insts.iteritems():
                    if self.driver.match_zk_info(cid, cinfo, zk):
                        return [cid]
                log.info("No leader found for component %s", component.name)
                log.info("Instances %s", insts)
                log.info("ZK info to match: %s", zk)
            else:
                log.warn("No leader info for %s", component.name)
        else:
            m = re.match(r"^(?P<num>[0-9]+)(?P<perc>%*)$", selector)
            if m:
                count = int(m.group("num"))
                if m.group("perc"):
                    count = max(1, count * len(insts) / 100)
        return insts.keys()[:count]
