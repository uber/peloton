import logging
import time

from client import Client

from peloton_client.pbgen.peloton.api.v0.host import host_pb2 as host
from peloton_client.pbgen.peloton.api.v0.host.svc import (
    host_svc_pb2 as hostsvc,
)

log = logging.getLogger(__name__)

draining_period_sec = 10


def start_maintenance(hosts):
    """
    starts Peloton host maintenance on the specified hosts
    :param hosts: list of hostnames
    :return: host_svc_pb2.StartMaintenanceResponse
    """
    client = Client()
    req = hostsvc.StartMaintenanceRequest(hostnames=hosts)

    resp = client.host_svc.StartMaintenance(
        req, metadata=client.hostmgr_metadata, timeout=10
    )
    return resp


def complete_maintenance(hosts):
    """
    completes Peloton host maintenance on the specified hosts
    :param hosts: list of hostnames
    :return: host_svc_pb2.CompleteMaintenanceResponse
    """
    client = Client()
    request = hostsvc.CompleteMaintenanceRequest(hostnames=hosts)

    resp = client.host_svc.CompleteMaintenance(
        request, metadata=client.hostmgr_metadata, timeout=10
    )
    return resp


def query_hosts(states):
    """
    query hosts by state
    :param states: list of host_pb2.HostStates
    :return: host_svc_pb2.QueryHostsResponse
    """
    if not states:
        states = [
            host.HOST_STATE_UP,
            host.HOST_STATE_DRAINING,
            host.HOST_STATE_DOWN,
        ]
    client = Client()
    request = hostsvc.QueryHostsRequest(host_states=states)

    resp = client.host_svc.QueryHosts(
        request, metadata=client.hostmgr_metadata, timeout=10
    )
    return resp


def wait_for_host_state(hostname, state):
    """
    Waits for the host to reach the specified state
    :param hostname: The hostname of the host
    :param state:  The host state to reach
    """
    attempts = 0
    max_retry_attempts = 10

    log.info("%s waiting for state %s", hostname, host.HostState.Name(state))
    start = time.time()
    while attempts < max_retry_attempts:
        try:
            if is_host_in_state(hostname, state):
                break
        except Exception as e:
            log.warn(e)
        finally:
            # Sleep for one draining cycle period
            time.sleep(draining_period_sec)
            attempts += 1

    if attempts == max_retry_attempts:
        log.info(
            "%s max attempts reached to wait for host state %s",
            hostname,
            host.HostState.Name(state),
        )
        assert False

    end = time.time()
    elapsed = end - start
    log.info("%s state transition took %s seconds", hostname, elapsed)


def is_host_in_state(hostname, state):
    resp = query_hosts([state])
    for host_info in resp.host_infos:
        if host_info.hostname == hostname:
            return True
    return False


def get_host_in_state(state):
    """
    returns a host in the specified state. Note that the caller should make sure
    there is at least one host in the the requested state.
    :param state: host_pb2.HostState
    :return: Hostname of a host in the specified state
    """
    resp = query_hosts([state])
    assert len(resp.host_infos) > 0
    return resp.host_infos[0].hostname
