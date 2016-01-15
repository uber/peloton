from __future__ import absolute_import

import os
import sys
import signal

from clay import config
from tornado.httpserver import HTTPServer
from tornado.netutil import bind_unix_socket
import tornado.autoreload
import tornado.ioloop
import tornado.web
from mutornadomon import initialize_mutornadomon


from peloton.rpc import set_worker_id
from peloton.rpc.req_handler import ServiceRequestHandler

log = config.get_logger(__name__)


def init_app():
    """Initialize the tornado application.

    Tornado settings are stored in the configuration.
    """
    for modulename in config.get('service.imports'):
        log.info('Loading Thrift service import: %s' % modulename)
        __import__(modulename)

    routes = ServiceRequestHandler.get_routes()
    log.info('Starting the app with the following routes=%s',
             [r[0] for r in routes])
    tornado_app = tornado.web.Application(routes, **config.get('tornado'))

    monitor = initialize_mutornadomon(tornado_app)
    add_shutdown_handler(monitor)

    return tornado_app


def add_shutdown_handler(monitor):
    """Gracefully stop the monitor and ioloop when process shuts down"""
    def shut_down(*args):
        monitor.stop()
        tornado.ioloop.IOLoop.current().stop()

    for sig in (signal.SIGQUIT, signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, shut_down)


def start_ioloop():
    """Serve traffic."""

    xheaders = config.get('server.xheaders', False)

    socket_location = config.get('socket.name')
    if len(sys.argv) > 1:
        socket_location = sys.argv[1]

    if len(sys.argv) > 2:
        worker_id = sys.argv[2]
        config.CONFIG.override({'worker_id': worker_id})
        log.info('Setting worker_id to %s', worker_id)
        set_worker_id(worker_id)

    if config.get('datacenter') is None:
        dc_file = '/etc/uber/datacenter'
        if os.path.exists(dc_file):
            with open(dc_file, 'r') as fp:
                datacenter = fp.read()
        else:
            datacenter = 'unknown'
        config.CONFIG.override({'datacenter': datacenter})

    app = init_app()
    if socket_location:
        server = HTTPServer(app, xheaders=xheaders)
        backlog = int(config.get('socket.backlog'))
        socket = bind_unix_socket(socket_location, mode=0666, backlog=backlog)
        log.info('Listening on socket %s', socket_location)
        server.add_socket(socket)
    else:
        set_worker_id(999)
        log.info('Listening on port %s', config.get('server.port'))
        app.listen(
            config.get('server.port'),
            xheaders=xheaders,
        )

    io_loop = tornado.ioloop.IOLoop.instance()

    io_loop.start()

if __name__ == '__main__':
    sys.exit(start_ioloop())
