#!/usr/bin/env python

import subprocess
import glob
import string
import os

# peloton_proto = './vendor/code.uber.internal/infra/peloton/protobuf/'
peloton_proto = './protobuf/'
go_loc = 'code.uber.internal/infra/peloton/.gen/'
gen_dir = '.gen'


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
        cmd = 'protoc --proto_path=%s --go_out=%s:%s %s' % \
              (peloton_proto, m, gen_dir, f)
        print cmd
        subprocess.call(cmd, shell=True)


if __name__ == '__main__':
    main()
