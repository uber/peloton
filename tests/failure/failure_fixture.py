import logging

from tests.failure.framework import components, framework
from tests.integration.common import wait_for_condition
from tests.integration.aurorabridge_test.client import Client as aurorabridge_client
from tests.integration import job as tjob
from tests.integration import stateless_job as sjob
from tests.integration import stateless_update


class FailureFixture(object):
    """
    Fixture for failure tests. It is responsible for creating
    and initializing the failure-testing framework. Each test
    gets an instance of the fixture using which the test can
    operate on the framework. In addition, the fixture provides
    helper functions to make writing tests easier.
    """

    # Number of attempts to make while waiting for a condition
    MAX_RETRY_ATTEMPTS = 180

    def __init__(self):
        self.fw = framework.FailureFramework()

        self.log = logging.getLogger(__name__)
        self.log.level = logging.INFO
        sh = logging.StreamHandler()
        sh.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
        )
        self.log.addHandler(sh)

        self.mesos_master = components.MesosMaster()
        self.mesos_agent = components.MesosAgent()
        self.zookeeper = components.Zookeeper()
        self.cassandra = components.Cassandra()
        self.hostmgr = components.HostMgr()
        self.jobmgr = components.JobMgr()
        self.resmgr = components.ResMgr()
        self.batch_pe = components.BatchPlacementEngine()
        self.stateless_pe = components.StatelessPlacementEngine()
        self.aurorabridge = components.AuroraBridge()

        self.integ_config = tjob.IntegrationTestConfig(
            max_retry_attempts=self.MAX_RETRY_ATTEMPTS
        )

    def setup(self):
        """
        Initializes the failure-test framework.
        """
        self.fw.setup()
        self.client = self.fw.client
        self.aurorabridge_client = aurorabridge_client()

    def teardown(self):
        """
        Cleans up state if any.
        """
        pass

    def reset_client(self):
        """
        Re-initialize PelotonClient. Useful after a leader change has
        happened.
        """
        self.fw.reset_client()
        self.client = self.fw.client

    def job(self, **kwargs):
        """
        Create the spec for a job with some defaults.
        :param kwargs: Keyword arguments for job spec
        """
        kwargs.setdefault("config", self.integ_config)
        kwargs.setdefault("client", self.client)
        return tjob.Job(**kwargs)

    def stateless_job(self, **kwargs):
        """
        Create the spec for a stateless job with some defaults.
        :param kwargs: Keyword arguments for stateless job spec
        """
        kwargs.setdefault("client", self.client)
        return sjob.StatelessJob(**kwargs)

    def stateless_update(self, *args, **kwargs):
        """
        Create the spec for a stateless update with some defaults.
        :param kwargs: Keyword arguments for stateless update spec
        """
        kwargs.setdefault("client", self.client)
        return stateless_update.StatelessUpdate(*args, **kwargs)

    def wait_for_condition(self, condition):
        """
        Wait for a condition to be true.
        :param condition: Function that is evalauted
        """
        wait_for_condition(
            message="", condition=condition, config=self.integ_config
        )

    def wait_for_leader_change(self, comp, old_leader):
        """
        Wait for the leader of a component to change.
        :param comp: Component to check
        :param old_leader: Zookeeper data for old leader
        """
        self.log.info(
            "%s: waiting for leader change. Old leader %s",
            comp.name,
            old_leader,
        )

        def leader_changed():
            new_leader = self.fw.get_leader_info(comp)
            self.log.debug("%s: leader info %s", comp.name, new_leader)
            if new_leader != old_leader:
                self.log.info(
                    "%s: leader changed to %s", comp.name, new_leader
                )
                return True

        self.wait_for_condition(leader_changed)
