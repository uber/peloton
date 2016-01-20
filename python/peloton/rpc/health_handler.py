from __future__ import absolute_import

from clay_tornado import BaseHandler
from clay import config

log = config.get_logger(__name__)


class HealthHandler(BaseHandler):
    def get(self):
        self.set_header('Content-Type', 'text/plain')
        self.write('OK\n')
        self.finish()


class HealthSentryHandler(BaseHandler):
    def get(self):
        log.warning('Health Sentry Endpoint called')
        self.set_header('Content-Type', 'text/plain')
        self.write('Health Sentry Endpoint Called\n')
        self.finish()
