import pytest
import uuid


from tests.integration.client import Client, with_private_stubs
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.peloton.private.hostmgr.hostsvc import hostsvc_pb2 as v0hostmgr
from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton
from tests.integration.conf_util import MESOS_AGENTS, create_task_cfg

# Mark test module so that we can run tests by tags
pytestmark = [
    pytest.mark.default,
    pytest.mark.hostmgr_internal,
]


# Test acquire host offers functionality.
# Request for hosts with specific set of filters and verify the response
# Return these offers back to hostmgr
def test__acquire_release_host_offers():
    resource_constraint = v0hostmgr.ResourceConstraint(
        minimum=task.ResourceConfig(cpuLimit=3.0))
    host_filter = v0hostmgr.HostFilter(
        resourceConstraint=resource_constraint,
        quantity=v0hostmgr.QuantityControl(maxHosts=2),
    )
    request = v0hostmgr.AcquireHostOffersRequest(filter=host_filter)
    client = with_private_stubs(Client())

    resp = client.hostmgr_svc.AcquireHostOffers(
        request,
        metadata=client.hostmgr_metadata,
        timeout=20)

    # max hosts is 2, we should expect 2 host offers
    assert len(resp.hostOffers) == 2
    for offer in resp.hostOffers:
        assert (offer.hostname in MESOS_AGENTS)

    # release offers to hostmgr
    resp = client.hostmgr_svc.ReleaseHostOffers(
        request=v0hostmgr.ReleaseHostOffersRequest(hostOffers=resp.hostOffers),
        metadata=client.hostmgr_metadata,
        timeout=20)

    assert resp.HasField("error") is False


# Test acquire host offers API errors


def test__acquire_return_offers_errors():
    resource_constraint = v0hostmgr.ResourceConstraint(
        minimum=task.ResourceConfig(cpuLimit=14.0))
    host_filter = v0hostmgr.HostFilter(
        resourceConstraint=resource_constraint)
    request = v0hostmgr.AcquireHostOffersRequest(filter=host_filter)
    # decorate the client to add peloton private API stubs
    client = with_private_stubs(Client())

    # ask is 14 cpus, so no hosts should match this
    resp = client.hostmgr_svc.AcquireHostOffers(
        request,
        metadata=client.hostmgr_metadata,
        timeout=20)
    assert len(resp.hostOffers) == 0

    # release offers to hostmgr with a invalid offer ID
    resp = client.hostmgr_svc.ReleaseHostOffers(
        request=v0hostmgr.ReleaseHostOffersRequest(
            hostOffers=[v0hostmgr.HostOffer(
                id=peloton.HostOfferID(value="invalid_id"))]
        ),
        metadata=client.hostmgr_metadata,
        timeout=20)

    assert resp.error is not None


# Test cluster capacity API


def test__cluster_capacity():
    # get cluster capacity
    client = with_private_stubs(Client())
    resp = client.hostmgr_svc.ClusterCapacity(
        request=v0hostmgr.ClusterCapacityRequest(),
        metadata=client.hostmgr_metadata,
        timeout=20)
    assert resp.HasField("error") is False

    # check capacity
    for resource in resp.physicalResources:
        assert resource.kind in ['cpu', 'gpu', 'memory', 'disk', 'fd']
        if resource.kind == 'cpu':
            assert resource.capacity == 12.0  # 4cpu * 3 agents
        if resource.kind == 'memory':
            assert resource.capacity == 6144.0  # 2048Mb * 3 agents

# Test cluster capacity API


def test__launch_kill():
    client = with_private_stubs(Client())

    # acquire 1 host offer
    resource_constraint = v0hostmgr.ResourceConstraint(
        minimum=task.ResourceConfig(cpuLimit=3.0))
    host_filter = v0hostmgr.HostFilter(
        resourceConstraint=resource_constraint,
        quantity=v0hostmgr.QuantityControl(maxHosts=1),
    )
    request = v0hostmgr.AcquireHostOffersRequest(
        filter=host_filter,
    )

    resp = client.hostmgr_svc.AcquireHostOffers(
        request,
        metadata=client.hostmgr_metadata,
        timeout=20)

    assert len(resp.hostOffers) == 1

    # launch a test task using this offer
    cmd = "echo 'succeeded instance task' & sleep 100"
    tc = task.TaskConfig(
        command=mesos.CommandInfo(shell=True, value=cmd),
        name="task_name",
        resource=task.ResourceConfig(cpuLimit=1.0),
    )
    tid = mesos.TaskID(value=str(uuid.uuid4())+'-1-1')
    t = v0hostmgr.LaunchableTask(
        taskId=tid,
        config=tc,
    )

    # Test 1
    # launch task using invalid offer
    req = v0hostmgr.LaunchTasksRequest(
        hostname=resp.hostOffers[0].hostname,
        agentId=resp.hostOffers[0].agentId,
        tasks=[t],
        id=peloton.HostOfferID(value=str(uuid.uuid4())))
    try:
        resp = client.hostmgr_svc.LaunchTasks(
            req,
            metadata=client.hostmgr_metadata,
            timeout=20)
        assert False, 'LaunchTasks should have failed'
    except:
        pass

    # Test 2
    # launch task using valid offer
    req = v0hostmgr.LaunchTasksRequest(
        hostname=resp.hostOffers[0].hostname,
        agentId=resp.hostOffers[0].agentId,
        tasks=[t],
        id=resp.hostOffers[0].id)
    resp = client.hostmgr_svc.LaunchTasks(
        req,
        metadata=client.hostmgr_metadata,
        timeout=20)
    assert resp.HasField("error") is False

    # Test 3
    # kill with empty TaskIDs list
    resp = client.hostmgr_svc.KillTasks(
        v0hostmgr.KillTasksRequest(
            taskIds=[]),
        metadata=client.hostmgr_metadata,
        timeout=20)
    assert resp.HasField("error") is True

    # Test 4
    # kill valid TaskID
    resp = client.hostmgr_svc.KillTasks(
        v0hostmgr.KillTasksRequest(taskIds=[tid]),
        metadata=client.hostmgr_metadata,
        timeout=20)
    assert resp.HasField("error") is False
