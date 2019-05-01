from abc import abstractmethod


class ClusterDriverABC(object):
    """
    The failure-testing framework can work with many different
    kinds of Peloton deployments (minicluster, vcluster etc(=) through
    the use of drivers. This class defines an abstract interface
    that must be implemented by all drivers.
    """

    @abstractmethod
    def setup(self, *args, **kwargs):
        """
        Initialize the driver for use.
        :param args: Positional arguments for driver setup. These
        arguments are driver specific, see a particular driver for
        details.
        :param kwargs: Keyword arguments for driver setup.  These
        arguments are driver specific, see a particular driver for
        details.
        """
        pass

    @abstractmethod
    def teardown(self, *args, **kwargs):
        """
        Cleanup driver state if any.
        :param args: Positional arguments for driver teardown. These
        arguments are driver specific, see a particular driver for
        details.
        :param kwargs: Keyword arguments for driver teardown. These
        arguments are driver specific, see a particular driver for
        details.
        """
        pass

    @abstractmethod
    def info(self):
        """
        Get information about Peloton components in the deployment. Mainly
        used for logging.
        :return: List of (string, string) pairs containing the
        component-name and textual information about the component.
        """
        pass

    @abstractmethod
    def find(self, component_name, running_only=True):
        """
        Find instances of a component.
        :param component_name: String. Name of component to search for
        :param running_only: Boolean. If true, return running components only
        :return: Dict of component instance-identifier to instance-information.
        The instance-identifier and instance-information objects are driver
        specific, see a particular driver for details.
        """
        pass

    @abstractmethod
    def start(self, component_instance_id):
        """
        Start instance of a component. Starting an already started component
        instance should not raise an exception.
        :param component_instance_id: Component instance-identifier. This is
        a driver-specific object, see a particular driver for details.
        """
        pass

    @abstractmethod
    def stop(self, component_instance_id):
        """
        Stop instance of a component. Stopping an already stopped component
        instance should not raise an exception.
        :param component_instance_id: Component instance-identifier. This is
        a driver-specific object, see a particular driver for details.
        """
        pass

    @abstractmethod
    def match_zk_info(
        self, component_instance_id, component_instance_info, zk_node_info
    ):
        """
        Check if a component instance matches the provided Zookeeper node data.
        :param component_instance_id: Component instance-identifier. This is
        a driver-specific object, see a particular driver for details.
        :param component_instance_info: Component instance information. This is
        a driver-specific object, see a particular driver for details.
        :param zk_node_info: Dict. Zookeeper node data to match against
        :return: True if the component instance matches.
        """
        pass

    @abstractmethod
    def execute(self, component_instance_id, *cmd_and_args):
        """
        Execute a command within a component instance.
        :param component_instance_id: Component instance-identifier. This is
        a driver-specific object, see a particular driver for details.
        :param cmd_and_args: Command name and arguments
        """
        pass

    @abstractmethod
    def get_peloton_client(self, name):
        """
        Get a PelotonClient for the cluster.
        TODO This function is required to work around a limitation of
        TODO minicluster.
        Once that is removed, replace this with a function to get the host:port
        of the Zookeeper component of Peloton cluster and move the
        responsibility of creating the client to the framework.
        :param name: String. Name to use for the client
        :return: PelotonClient object
        """
        pass
