from __future__ import absolute_import


SERVICE_IMPL_MAP = {}


def thrift_service(uri):
    """Decorator to register an implementation class for a service in Thrift IDL"""

    class ClassWrapper:
        def __init__(self, cls):
            svc = getattr(cls, '__thrift_service__', None)
            if svc is None or not isinstance(svc, type):
                raise Exception('Missing __thrift_service__ attr for %s' % cls)
            self.cls = cls

        def __call__(self, *args, **kwargs):
            svc_impl = self.cls(*args, **kwargs)

            if uri in SERVICE_IMPL_MAP:
                raise Exception('Duplicate service URI %s' % uri)

            SERVICE_IMPL_MAP[uri] = svc_impl
            return svc_impl

    return ClassWrapper
