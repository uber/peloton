from __future__ import absolute_import

from peloton.rpc.req_handler import ServiceRequestHandler
from peloton.rpc.health_handler import HealthHandler, HealthSentryHandler


ROUTES = [
    (r'/health', HealthHandler),
    (r'/health/sentry', HealthSentryHandler),
]


def get_routes():
    routes = ServiceRequestHandler.get_routes()
    routes.extend(ROUTES)
    return routes
