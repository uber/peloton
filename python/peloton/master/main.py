from __future__ import absolute_import

from clay import config
from peloton.rpc import server

log = config.get_logger(__name__)


def serve():
    """Serve traffic."""

    server.start_ioloop()
