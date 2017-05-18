#!/usr/bin/env python

import subprocess
import glob
import string
import os
import sys

# peloton_proto = './vendor/code.uber.internal/infra/peloton/protobuf/'
peloton_proto = './protobuf/'
go_loc = 'code.uber.internal/infra/peloton/.gen/'
gen_dir = '.gen'
protoc_cmd = (
    'protoc --proto_path={proto_path} --go_out={mflag}:{gen_dir} {file}'
)


def protos():
    f = []
    # Py2 glob has no **
    for g in ['*/*/*.proto', '*/*/*/*.proto', '*/*/*/*/*.proto']:
        f += glob.glob(peloton_proto + g)
    return f


def mflags(files):
    pfiles = [string.replace(f, peloton_proto, '') for f in files]
    pfiles.remove('peloton/api/peloton.proto')
    m = string.join(['M' + f + '=' + go_loc +
                     os.path.dirname(f) for f in pfiles], ',')
    m += ',Mpeloton/api/peloton.proto=%speloton/api/peloton' % go_loc
    return m


def main():

    files = protos()
    m = mflags(files)

    # For every .proto file in peloton generate us a golang file
    for f in files:
        print protoc_cmd.format(proto_path=peloton_proto, mflag='${mflag}',
                                gen_dir=gen_dir, file=f)
        cmd = protoc_cmd.format(proto_path=peloton_proto, mflag=m,
                                gen_dir=gen_dir, file=f)
        retval = subprocess.call(cmd, shell=True)
        if retval != 0:
            sys.exit(retval)


if __name__ == '__main__':
    main()
