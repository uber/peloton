#!/usr/bin/env python2

import argparse
import glob
import os
import string
import subprocess
import sys
from distutils.sysconfig import get_python_lib

peloton_proto = "./protobuf/"
proto_file_paths = [
    "*/*/*.proto",
    "*/*/*/*.proto",
    "*/*/*/*/*.proto",
    "*/*/*/*/*/*.proto",
    "*/*/*/*/*/*/*.proto",
]
protoc_cmd = (
    "protoc --proto_path /usr/local/include/ --proto_path={proto_path} "
    "--{gen}_out={mflags}:{out_dir} --{gen}_opt={gen_opt} {file}"
)

protoc_python_cmd = (
    "python -m grpc_tools.protoc  --proto_path={proto_path} "
    "--{gen}_out={out_dir} --grpc_{gen}_out={out_dir} {file}"
)

doc_opt = (
    "markdown,api-reference.md:mesos/*,private/*,api/v0/*,timestamp.proto"
)


def protos():
    f = []
    # Py2 glob has no **
    for g in proto_file_paths:
        f += glob.glob(peloton_proto + g)
    return f


def mflags(files, go_loc):
    pfiles = [string.replace(f, peloton_proto, "") for f in files]
    pfiles.remove("peloton/api/v0/peloton.proto")
    pfiles.remove("peloton/api/v1alpha/peloton.proto")

    m = "plugins=grpc,"
    m += string.join(
        ["M" + f + "=" + go_loc + os.path.dirname(f) for f in pfiles], ","
    )
    m += ",Mpeloton/api/v0/peloton.proto=%speloton/api/v0/peloton" % go_loc
    m += (
        ",Mpeloton/api/v1alpha/peloton.proto="
        "%speloton/api/v1alpha/peloton" % go_loc
    )
    m += (
        ",Mqos/v1alpha1/qos.proto="
        "%sqos/v1alpha1/qos" % go_loc
    )
    return m


def is_service_proto(f):
    with open(f) as o:
        lines = o.readlines()

        for l in lines:
            if l.startswith("service "):
                return True
        return False


def generate(gen, f, m, out_dir, gen_opt=""):
    print(
        " ".join(
            [
                "protoc",
                "--proto_path /usr/local/include/",
                "--proto_path=%s" % peloton_proto,
                "--%s_out=%s:%s"
                % (gen, "${mflags}" if m != "" else m, out_dir),
                "--%s_opt=%s" % (gen, gen_opt) if gen_opt != "" else "",
                f,
            ]
        )
    )

    cmd = protoc_cmd.format(
        proto_path=peloton_proto,
        gen=gen,
        mflags=m,
        out_dir=out_dir,
        gen_opt=gen_opt,
        file=f,
    )
    retval = subprocess.call(cmd, shell=True)

    if retval != 0:
        sys.exit(retval)


def clean_python_stub(stub_directory):
    print "cleaning old python stub generation"
    retval = subprocess.call("rm -rf " + stub_directory, shell=True)
    if retval != 0:
        sys.exit(retval)


def generate_python_stub(gen, stub_dir, f):
    cmd = protoc_python_cmd.format(
        proto_path=peloton_proto,
        gen=gen,
        out_dir=stub_dir,
        file=f
    )
    print cmd
    retval = subprocess.call(cmd, shell=True)
    if retval != 0:
        sys.exit(retval)


def create_init_files(stub_directory):
    print "creating init files"
    for root, dir, files in os.walk(stub_directory):
        retval = subprocess.call("touch " + root + "/__init__.py", shell=True)
        if retval != 0:
            sys.exit(1)


def patch_python_stub(stub_directory):
    retval = subprocess.call(
        ["bash", "scripts/patch-python-stub.sh", stub_directory])
    if retval != 0:
        sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate types, yarpc stubs and doc from protobuf files"
    )
    parser.add_argument(
        "-l",
        "--go-loc",
        help="go location of generated code",
        default="github.com/uber/peloton/.gen/",
    )
    parser.add_argument(
        "-o", "--out-dir", help="output dir of generated code,outdir for Python code is relative to site-packages", default=".gen"
    )
    parser.add_argument(
        "-g",
        "--generator",
        help="protoc generator to use" "(go, doc)",
        default="go",
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    files = protos()

    if args.generator == "go":
        m = mflags(files, args.go_loc)

        # For every .proto file in peloton generate us a golang file
        for f in files:
            generate("go", f, m, args.out_dir)

            # Generate yarpc-go files for protobuf files with a service.
            # The yarpc plugin generates bad output for files without any
            # services.
            if is_service_proto(f):
                generate("yarpc-go", f, m, args.out_dir)

    elif args.generator == "doc":
        generate("doc", " ".join(files), "", args.out_dir, doc_opt)

    elif args.generator == "python":
        # For every .proto file in peloton generate us a python file
        stub_directory = get_python_lib() + "/" + args.out_dir
        # clearing python stub
        clean_python_stub(stub_directory)
        # creating directory if not exist
        retval = subprocess.call("mkdir -p " + stub_directory, shell=True)
        if retval != 0:
            sys.exit(1)
        print "generating python stub"
        for f in files:
            generate_python_stub("python", stub_directory, f)
        create_init_files(stub_directory)
        print "patching python stub"
        patch_python_stub(stub_directory)


if __name__ == "__main__":
    main()
