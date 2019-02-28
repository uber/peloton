import logging
import time

from client import Client
from peloton_client.pbgen.peloton.api.v0.respool import respool_pb2 as respool
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton

RESPOOL_ROOT = '/'
log = logging.getLogger(__name__)


class Pool(object):
    """
    Pool represents a peloton resource pool
    """
    def __init__(self, config, client=None):
        self.config = config
        self.client = client or Client()
        self.id = None

    def ensure_exists(self):
        """
        creates a resource pool if it doesn't exist based on the config
        :return: resource pool ID
        """
        respool_name = self.config.respool_config.name
        request = respool.LookupRequest(
            path=respool.ResourcePoolPath(value=RESPOOL_ROOT + respool_name),
        )
        resp = self.client.respool_svc.LookupResourcePoolID(
            request,
            metadata=self.client.resmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )
        if resp.id.value is None or resp.id.value == u'':
            request = respool.CreateRequest(
                config=self.config.respool_config,
            )
            deadline = time.time() + self.config.rpc_timeout_sec
            while time.time() < deadline:
                resp = self.client.respool_svc.CreateResourcePool(
                    request,
                    metadata=self.client.resmgr_metadata,
                    timeout=self.config.rpc_timeout_sec,
                )
                if resp.HasField('error'):
                    time.sleep(0.5)
                    continue
                break
            else:
                assert False, resp
            id = resp.result.value
            log.info('created respool %s (%s)', respool_name, id)
        else:
            id = resp.id.value
            log.info('found respool %s (%s)', respool_name, id)

        assert id
        self.id = id
        return self.id

    def pool_info(self):
        """
        :return: the resource pool info
        """
        assert self.id, "No resource pool ID defined"
        request = respool.GetRequest(
            id=peloton.ResourcePoolID(value=self.id),
        )
        resp = self.client.respool_svc.GetResourcePool(
            request,
            metadata=self.client.resmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )

        assert not resp.HasField('error'), resp

        return resp.poolinfo

    def delete(self):
        """
        deletes the resource pool, all the jobs in the pool must be stopped
        before calling it.
        """
        if not self.id:
            return

        respool_name = self.config.respool_config.name

        request = respool.DeleteRequest(
            path=respool.ResourcePoolPath(value=RESPOOL_ROOT + respool_name),
        )
        resp = self.client.respool_svc.DeleteResourcePool(
            request,
            metadata=self.client.resmgr_metadata,
            timeout=self.config.rpc_timeout_sec,
        )

        assert not resp.HasField('error'), resp
        log.info('deleted respool: %s', respool_name)
