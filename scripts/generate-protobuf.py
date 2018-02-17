#!/usr/bin/env python

import argparse
import glob
import os
import string
import subprocess
import sys

peloton_proto = './protobuf/'
protoc_cmd = (
    'protoc  --proto_path={proto_path} --{gen}_out={mflags}:{out_dir} '
    '--{gen}_opt={gen_opt} {file}'
)
doc_opt = 'html,apidoc.html:mesos/*,private/*'


def protos():
    f = []
    # Py2 glob has no **
    for g in ['*/*/*.proto', '*/*/*/*.proto', '*/*/*/*/*.proto']:
        f += glob.glob(peloton_proto + g)
    return f


def mflags(files, go_loc):
    pfiles = [string.replace(f, peloton_proto, '') for f in files]
    pfiles.remove('peloton/api/peloton.proto')
    m = string.join(['M' + f + '=' + go_loc +
                     os.path.dirname(f) for f in pfiles], ',')
    m += ',Mpeloton/api/peloton.proto=%speloton/api/peloton' % go_loc
    return m


def is_service_proto(f):
    with open(f) as o:
        lines = o.readlines()

        for l in lines:
            if l.startswith('service '):
                return True
        return False


def generate(gen, f, m, out_dir, gen_opt=''):
    print ' '.join([
        'protoc',
        '--proto_path=%s' % peloton_proto,
        '--%s_out=%s:%s' % (gen, '${mflags}' if m != '' else m, out_dir),
        '--%s_opt=%s' % (gen, gen_opt) if gen_opt != '' else '',
        f,
    ])

    cmd = protoc_cmd.format(proto_path=peloton_proto, gen=gen, mflags=m,
                            out_dir=out_dir, gen_opt=gen_opt, file=f)
    retval = subprocess.call(cmd, shell=True)

    if retval != 0:
        sys.exit(retval)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Generate types, yarpc stubs and doc from protobuf files')
    parser.add_argument('-l', '--go-loc', help='go location of generated code',
                        default='code.uber.internal/infra/peloton/.gen/')
    parser.add_argument('-o', '--out-dir', help='output dir of generated code',
                        default='.gen')
    parser.add_argument('-g', '--generator', help='protoc generator to use'
                        '(go, doc)',  default='go')

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    files = protos()

    if args.generator == 'go':
        m = mflags(files, args.go_loc)

        # For every .proto file in peloton generate us a golang file
        for f in files:
            generate('go', f, m, args.out_dir)

            # Generate yarpc-go files for protobuf files with a service.
            # The yarpc plugin generates bad output for files without any
            # services.
            if is_service_proto(f):
                generate('yarpc-go', f, m, args.out_dir)

    elif args.generator == 'doc':
        generate('doc', ' '.join(files), '', args.out_dir, doc_opt)


if __name__ == '__main__':
    main()
