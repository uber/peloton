import logging
import time

from client import Client

from peloton_client.pbgen.peloton.api.v0.host import host_pb2 as host
from peloton_client.pbgen.peloton.api.v0.host.svc import (
    host_svc_pb2 as hostsvc,
)
from tests.integration.conf_util import HOSTPOOL_DEFAULT

from retry import retry

log = logging.getLogger(__name__)

draining_period_sec = 5


def start_maintenance(hosts, client=None):
    """
    starts Peloton host maintenance on the specified hosts
    :param hosts: list of hostnames
    :return: host_svc_pb2.StartMaintenanceResponse
    """
    c = client or Client()
    req = hostsvc.StartMaintenanceRequest(hostnames=hosts)

    resp = c.host_svc.StartMaintenance(
        req, metadata=c.hostmgr_metadata, timeout=10
    )
    return resp


def complete_maintenance(hosts, client=None):
    """
    completes Peloton host maintenance on the specified hosts
    :param hosts: list of hostnames
    :return: host_svc_pb2.CompleteMaintenanceResponse
    """
    c = client or Client()
    request = hostsvc.CompleteMaintenanceRequest(hostnames=hosts)

    resp = c.host_svc.CompleteMaintenance(
        request, metadata=c.hostmgr_metadata, timeout=10
    )
    return resp


def query_hosts(states, client=None):
    """
    query hosts by state
    :param states: list of host_pb2.HostStates
    :return: host_svc_pb2.QueryHostsResponse
    """
    c = client or Client()
    if not states:
        states = [
            host.HOST_STATE_UP,
            host.HOST_STATE_DRAINING,
            host.HOST_STATE_DOWN,
        ]
    request = hostsvc.QueryHostsRequest(host_states=states)

    resp = c.host_svc.QueryHosts(
        request, metadata=c.hostmgr_metadata, timeout=10
    )
    return resp


def wait_for_host_state(hostname, state):
    """
    Waits for the host to reach the specified state
    :param hostname: The hostname of the host
    :param state:  The host state to reach
    """
    attempts = 0
    max_retry_attempts = 20

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
    resp = query_hosts(None)
    for host_info in resp.host_infos:
        if host_info.hostname == hostname:
            log.info("host state: %s, expected: %s", host_info.state, state)
            return host_info.state == state
    return False


def get_host_in_state(state, client=None):
    """
    returns a host in the specified state. Note that the caller should make sure
    there is at least one host in the the requested state.
    :param state: host_pb2.HostState
    :return: Hostname of a host in the specified state
    """
    c = client or Client()
    resp = query_hosts([state], c)
    assert len(resp.host_infos) > 0
    return resp.host_infos[0].hostname


def list_host_pools(client=None):
    """
    list all host pools.
    :param client: optional peloton client
    :return: host_svc_pb2.ListHostPoolsResponse
    """
    c = client or Client()
    request = hostsvc.ListHostPoolsRequest()

    resp = c.host_svc.ListHostPools(
        request, metadata=c.hostmgr_metadata, timeout=10
    )
    return resp


def create_host_pool(pool_id, client=None):
    """
    creates a host pool with given host pool id.
    :param pool_id: host pool id
    :param client: optional peloton client
    :return: host_svc_pb2.CreateHostPoolResponse
    """
    c = client or Client()
    request = hostsvc.CreateHostPoolRequest(name=pool_id)

    resp = c.host_svc.CreateHostPool(
        request, metadata=c.hostmgr_metadata, timeout=10
    )
    return resp


def delete_host_pool(pool_id, client=None):
    """
    deletes host pool with given host pool id.
    :param pool_id: host pool id
    :param client: optional peloton client
    :return: host_svc_pb2.DeleteHostPoolResponse
    """
    c = client or Client()
    request = hostsvc.DeleteHostPoolRequest(name=pool_id)

    resp = c.host_svc.DeleteHostPool(
        request, metadata=c.hostmgr_metadata, timeout=10
    )
    return resp


def change_host_pool(hostname, src_pool, dest_pool, client=None):
    """
    changes host pool of a host from source pool to destination pool
    :param hostname: hostname of the target host
    :param src_pool: source host pool id
    :param dest_pool: destination host pool id
    :param client: optional peloton client
    :return: host_svc_pb2.ChangeHostPoolResponse
    """
    c = client or Client()
    request = hostsvc.ChangeHostPoolRequest(
        hostname=hostname, sourcePool=src_pool, destinationPool=dest_pool)

    resp = c.host_svc.ChangeHostPool(
        request, metadata=c.hostmgr_metadata, timeout=10
    )
    return resp


@retry(tries=50, delay=1)
def ensure_host_pool(pool_name, num_hosts, client=None):
    """
    ensure host pool exists with at least required number of hosts.
    :param pool_name: target host pool name
    :param num_hosts: number of hosts required in target host pool
    :param client: optional peloton client
    """
    log.info('ensure at least {} hosts in {} host pool'.format(
        num_hosts, pool_name))

    existing_pool = None
    default_pool = None
    num_existing_hosts = 0

    # List all existing host pools.
    resp = list_host_pools(client=client)
    if not resp:
        raise Exception('List host pools failed: {}'.format(resp))

    # Get existing target host pool and default host pool.
    for pool in resp.pools:
        if pool.name == pool_name:
            existing_pool = pool
        if pool.name == HOSTPOOL_DEFAULT:
            default_pool = pool

    # Check if default host pool exists.
    if default_pool is None:
        raise Exception(
            'Missing default host pool, cluster is not initialized properly')

    # Check if target host pool exists and
    # update number of existing hosts in target host pool.
    if existing_pool is not None:
        num_existing_hosts = len(existing_pool.hosts)

    # Check if there are enough hosts to fulfil the request.
    num_more_hosts_required = num_hosts - num_existing_hosts
    if num_more_hosts_required <= 0:
        return
    if len(default_pool.hosts) < num_more_hosts_required:
        raise Exception(
            'No enough hosts to move to {} host pool: '
            '{} hosts in default host pool, {} more hosts required'.format(
                pool_name, len(default_pool.hosts), num_more_hosts_required))

    # Create new host pool if not exists.
    if existing_pool is None:
        resp = create_host_pool(pool_name, client=client)
        if not resp:
            raise Exception(
                'Create {} host pool failed: {}'.format(pool_name, resp))

    # Change host pool of hosts to the given pool.
    for i in range(num_more_hosts_required):
        hostname = default_pool.hosts[i]
        resp = change_host_pool(
            hostname,
            HOSTPOOL_DEFAULT,
            pool_name,
            client=client)
        if not resp:
            raise Exception(
                'Change host pool of {} failed: {}'.format(hostname, resp))


def cleanup_other_host_pools(pool_names, client=None):
    """
    Deletes all host-pools other than the specified pools. Hosts from deleted
    pools go to the "default" pool
    :param pool_names: List host pools to keep.
    :param client: optional peloton client
    """
    # List all existing host pools.
    resp = list_host_pools(client=client)
    if not resp:
        raise Exception('List host pools failed: {}'.format(resp))

    failed = []
    for pool in resp.pools:
        if pool.name == HOSTPOOL_DEFAULT or pool.name in pool_names:
            continue
        log.info("Deleting pool {}".format(pool.name))
        resp = delete_host_pool(pool.name, client=client)
        if not resp:
            log.info("Failed to delete pool {}: {}".format(pool.name, resp))
            failed.append(pool.name)
    if failed:
        raise Exception("Delete pool failed for %s" % ",".join(failed))
