import pytest

from tests.integration.client import Client, with_private_stubs
from peloton_client.pbgen.peloton.api.v1alpha.pod import pod_pb2 as pod
from peloton_client.pbgen.peloton.private.hostmgr.v1alpha import hostmgr_pb2 as v1hostmgr
from peloton_client.pbgen.peloton.private.hostmgr.v1alpha.svc import hostmgr_svc_pb2 as v1hostmgr_svc
from peloton_client.pbgen.peloton.api.v1alpha import peloton_pb2 as v1alpha

pytestmark = [
    pytest.mark.k8s,
]


class TestHostMgrV1alpha(object):

    @property
    def metadata(self):
        return self.client.hostmgr_metadata

    @property
    def client(self):
        return with_private_stubs(Client())

    def new_resource_constraint(self, cpu):
        return v1hostmgr.ResourceConstraint(
            minimum=pod.ResourceSpec(cpu_limit=cpu),
        )

    def test__acquire_leases(self):
        resource_constraint = self.new_resource_constraint(3.0)
        host_filter = v1hostmgr.HostFilter(
            resource_constraint=resource_constraint,
            max_hosts=2,
        )
        request = v1hostmgr_svc.AcquireHostsRequest(filter=host_filter)
        resp = self.client.v1hostmgr_svc.AcquireHosts(request,
                                                      metadata=self.metadata,
                                                      timeout=20)

        assert len(resp.hosts) == 2

    def test__acquire_return_hosts_error(self):
        resource_constraint = self.new_resource_constraint(14000.0)
        host_filter = v1hostmgr.HostFilter(
            resource_constraint=resource_constraint,
        )
        request = v1hostmgr_svc.AcquireHostsRequest(filter=host_filter)
        resp = self.client.v1hostmgr_svc.AcquireHosts(request,
                                                      metadata=self.metadata,
                                                      timeout=20)
        assert len(resp.hosts) == 0

        request = v1hostmgr_svc.TerminateLeasesRequest(
            leases=[
                v1hostmgr_svc.TerminateLeasesRequest.LeasePair(
                    lease_id=v1hostmgr.LeaseID(value='invalid lease id'),
                    hostname='invalid host name',
                ),
            ],
        )

        with pytest.raises(Exception):
            resp = self.client.v1hostmgr_svc.TerminateLeases(request,
                                                             metadata=self.metadata,
                                                             timeout=20)

    def test__cluster_capacity(self):
        resp = self.client.v1hostmgr_svc.ClusterCapacity(
            request=v1hostmgr_svc.ClusterCapacityRequest(),
            metadata=self.metadata,
            timeout=20,
        )

        for resource in resp.allocation:
            assert resource.kind in ['cpu', 'gpu', 'memory', 'disk', 'fd']
            assert resource.capacity == 0

        for resource in resp.capacity:
            assert resource.kind in ['cpu', 'gpu', 'memory', 'disk', 'fd']
            if resource.kind == 'cpu':
                assert resource.capacity > 0
            elif resource.kind == 'disk':
                assert resource.capacity > 0
            elif resource.kind == 'gpu':
                assert resource.capacity == 0
            elif resource.kind == 'memory':
                assert resource.capacity > 0

        for resource in resp.slack_allocation:
            assert resource.kind in ['cpu', 'gpu', 'memory', 'disk', 'fd']

        for resource in resp.slack_capacity:
            assert resource.kind in ['cpu', 'gpu', 'memory', 'disk', 'fd']

    def test__launch_kill(self):
        resource_constraint = self.new_resource_constraint(3.0)
        host_filter = v1hostmgr.HostFilter(
            resource_constraint=resource_constraint,
            max_hosts=1,
        )
        request = v1hostmgr_svc.AcquireHostsRequest(filter=host_filter)
        resp = self.client.v1hostmgr_svc.AcquireHosts(request,
                                                      metadata=self.metadata,
                                                      timeout=20)

        assert len(resp.hosts) == 1

        pod_spec = pod.PodSpec(
            pod_name=v1alpha.PodName(value='test-123'),
            containers=[
                pod.ContainerSpec(
                    name='c1',
                    image='alpine:3.6',
                    entrypoint=pod.CommandSpec(
                        value='/bin/sh',
                        arguments=[
                            '-c',
                            'while true; do echo OK && sleep 3; done',
                        ],
                    ),
                ),
            ],
        )
        pod_obj = v1hostmgr.LaunchablePod(
            pod_id=v1alpha.PodID(value='test-123'),
            spec=pod_spec,
        )
        req = v1hostmgr_svc.LaunchPodsRequest(
            lease_id=resp.hosts[0].lease_id,
            hostname=resp.hosts[0].host_summary.hostname,
            pods=[pod_obj],
        )

        self.client.v1hostmgr_svc.LaunchPods(
            req,
            metadata=self.metadata,
            timeout=20,
        )

        # the second launch pods call should fail.
        with pytest.raises(Exception):
            self.client.v1hostmgr_svc.LaunchPods(
                req,
                metadata=self.metadata,
                timeout=20,
            )

        req = v1hostmgr_svc.KillPodsRequest(
            pod_ids=[
                v1alpha.PodID(value='test-123'),
            ],
        )
        self.client.v1hostmgr_svc.KillPods(
            req,
            metadata=self.metadata,
            timeout=20,
        )
