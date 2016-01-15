from __future__ import absolute_import

import sys
import thriftrw

from . import IDL_PATH

IDL_FILE = 'config_bundle.thrift'

sys.modules[__name__] = thriftrw.load('%s/%s' % (IDL_PATH, IDL_FILE))

