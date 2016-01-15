from __future__ import absolute_import

from clay import config
from clay.ext.tornado import BaseHandler
from raven.contrib.tornado import SentryMixin
from tornado.gen import coroutine, maybe_future
from tornado.web import HTTPError

from peloton.rpc import SERVICE_IMPL_MAP

log = config.get_logger(__name__)


class ServiceRequestHandler(SentryMixin, BaseHandler):

    @classmethod
    def get_routes(cls):
        return [(path, cls) for path in SERVICE_IMPL_MAP.keys()]

    @coroutine
    def post(self):
        # Lookup service impl to dispatch the invocation
        uri = self.request.uri
        svc_impl = SERVICE_IMPL_MAP.get(uri)
        if not svc_impl:
            raise HTTPError(404, 'Invalid service URI %s' % uri)

        thrift_svc = svc_impl.__thrift_service__
        assert thrift_svc

        request_json = self.request.json

        # Lookup the service method
        self.method_name = request_json.get('method', None)
        if self.method_name is None:
            raise HTTPError(405, 'Missing method name')

        thrift_func = getattr(thrift_svc, self.method_name, None)
        if not thrift_func:
            raise HTTPError(405, 'Invalid method %s ' % self.method_name)

        svc_impl_func = getattr(svc_impl, self.method_name, None)
        if not svc_impl_func or not callable(svc_impl_func):
            raise HTTPError(501, 'Method %s not implemented' % self.method_name)

        # Deserialize the request params
        params = request_json.get('params')
        thrift_req = thrift_func.request.from_primitive(params)
        kwargs = {
            key: getattr(thrift_req, key, None) for key in params.keys()
        }

        # Invoke the method on service implementation
        try:
            result = yield maybe_future(svc_impl_func(**kwargs))
            if result is None:
                if thrift_func.response.type_spec.return_spec is not None:
                    # Thrift function should return a value
                    raise HTTPError(500, 'Missing return value for %s' %
                                    self.method_name)
                response = thrift_func.response()
            else:
                response = thrift_func.response(success=result)
        except Exception as e:
            exc_response = None
            for exc_field_spec in thrift_func.response.type_spec.exception_specs:
                exc_class = exc_field_spec.spec.surface
                if isinstance(e, exc_class):
                    exc_response = thrift_func.response(**{exc_field_spec.name: e})

            if exc_response:
                self.send_error(500, error_dict=exc_response.to_primitive())
                return
            else:
                # unrecognized exception was thrown. handle it
                log.exception('Unknown exception while invoking %s.%s' % (
                    svc_impl.__class__.__name__, self.method_name))
                raise e

        # Serialize the response and send it to client
        self.write_json(response.to_primitive())

    def _handle_request_exception(self, e):
        """Overwrite in order to log the error message and exception stack.
        """
        log.error("%s: method %s failed with error '%s'" % (
            self.request.uri, self.method_name, str(e)))

        super(ServiceRequestHandler, self)._handle_request_exception(e)
