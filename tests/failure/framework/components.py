from peloton_client.service.discovery import (
    zk_leader_path,
    job_manager_role,
    res_manager_role,
    host_manager_role,
)


class Component(object):
    """
    Base class for the different entities that make up a Peloton
    cluster such as Mesos master, Job Manager, Host Manager etc.
    Derived classes are defined for each kind of component.
    Each component has a user-friendly name and optionally the
    path of the Zookeeper node that stores the leader information
    for that component.
    """

    def __init__(self, name):
        self.name = name
        self.zk_leader_path = None


class MesosAgent(Component):
    """
    Mesos slave component.
    """

    def __init__(self):
        super(MesosAgent, self).__init__("mesos-agent")


class MesosMaster(Component):
    """
    Mesos master component.
    """

    def __init__(self):
        super(MesosMaster, self).__init__("mesos-master")


class Zookeeper(Component):
    """
    Zookeeper component.
    """

    def __init__(self):
        super(Zookeeper, self).__init__("zookeeper")


class Cassandra(Component):
    """
    Cassandra component.
    """

    def __init__(self):
        super(Cassandra, self).__init__("cassandra")


class HostMgr(Component):
    """
    Host manager component.
    """

    def __init__(self):
        super(HostMgr, self).__init__("hostmgr")
        self.zk_leader_path = zk_leader_path.format(role=host_manager_role)


class ResMgr(Component):
    """
    Resource manager component.
    """

    def __init__(self):
        super(ResMgr, self).__init__("resmgr")
        self.zk_leader_path = zk_leader_path.format(role=res_manager_role)


class JobMgr(Component):
    """
    Job manager component.
    """

    def __init__(self):
        super(JobMgr, self).__init__("jobmgr")
        self.zk_leader_path = zk_leader_path.format(role=job_manager_role)


class BatchPlacementEngine(Component):
    """
    Placement engine component for batch jobs.
    """

    def __init__(self):
        super(BatchPlacementEngine, self).__init__("placement-batch")


class StatelessPlacementEngine(Component):
    """
    Placement engine component for stateless jobs.
    """

    def __init__(self):
        super(StatelessPlacementEngine, self).__init__("placement-stateless")


class AuroraBridge(Component):
    """
    Aurorabridge component.
    """

    def __init__(self):
        super(AuroraBridge, self).__init__("aurora-bridge")
