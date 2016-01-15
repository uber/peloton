from __future__ import absolute_import

from clay import config
from tornado import gen, ioloop, httpclient
import ujson

log = config.get_logger(__name__)


class ThriftHttpClient(object):

    def __init__(self, host, port, io_loop=None):
        self.host = host
        self.port = port
        self.http_client = httpclient.AsyncHTTPClient()
        self.io_loop = io_loop or ioloop.IOLoop.current()
        self.service_url_map = {}

    def add_service(self, service, uri):
        self.service_url_map[service] = 'http://%s:%s%s' % (self.host, self.port, uri)
        return self

    @gen.coroutine
    def invoke(self, service, method, **kwargs):
        url = self.service_url_map.get(service, None)
        if url is None:
            raise ValueError('Invalid service %s' % service.__name__)

        thrift_func = getattr(service, method, None)
        if thrift_func is None:
            raise ValueError('Invalid method %s for service %s' % (
                method, service.__name__))

        request_dict = {
            'method': method,
            'params': thrift_func.request(**kwargs).to_primitive()
        }
        body = ujson.dumps(request_dict)

        request = httpclient.HTTPRequest(
            url=url,
            method='POST',
            headers={
                'Content-Type': 'application/json',
                'Content-Length': str(len(body)),
                'User-Agent': 'Python/ThriftHttpClient',
            },
            body=body,
        )

        try:
            response = yield self.http_client.fetch(request)
        except httpclient.HTTPError as e:
            if e.response is None or e.response.code != 500:
                # Raise non-thrift exceptions as HTTPError
                raise e

            try:
                resp_dict = ujson.loads(e.response.body) if e.response.body else None
                thrift_response = thrift_func.response.from_primitive(resp_dict)
            except Exception:
                # Failed to deserialize thrift exception, raise as HTTPError
                raise e

            # Raise deserialized thrift exception
            for exc_field_spec in thrift_response.type_spec.exception_specs:
                thrift_exc = getattr(thrift_response, exc_field_spec.name, None)
                if thrift_exc:
                    raise thrift_exc

            # Got an unknown exception, raise as HTTPError
            raise e

        except Exception as e:
            # Raise non-HTTP error as-is
            raise e

        assert response.code == 200
        try:
            resp_dict = ujson.loads(response.body) if response.body else None
            thrift_response = thrift_func.response.from_primitive(resp_dict)
        except Exception as e:
            # Raise deserialization exception
            log.exception('Failed to deserialize response %s' % response.body)
            raise e

        result = getattr(thrift_response, 'success', None)
        raise gen.Return(result)
