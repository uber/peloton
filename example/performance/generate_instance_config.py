import argparse
import yaml
from shutil import copyfile


def generate(val, sleep):
    k = {}
    for i in range(val):
        r = {}
        name = "instance" + str(i)
        r['name'] = name
        c = {}
        c['user'] = 'apoorva'
        c['shell'] = True
        c['value'] = 'echo Hello instance 0 && sleep ' + str(sleep)
        r['command'] = c
        t = {}
        t['cpuLimit'] = 0.1
        t['diskLimitMb'] = 10
        t['fdLimit'] = 10
        t['gpuLimit'] = 0
        t['memLimitMb'] = 2.0
        r['resource'] = t
        k[i] = r
    li = {}
    li['instanceConfig'] = k
    li['instanceCount'] = val
    y = yaml.dump(li, default_flow_style=False)
    return y


def main(val, sleep):
    copyfile('example/performance/testjob_base.yaml', 'tmp.yaml')
    y = generate(val, sleep)
    with open('tmp.yaml', 'a') as f:
        f.write(y)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate instance configurations.')
    parser.add_argument(
        '--number-instances',
        action="store",
        dest='val',
        type=int)
    parser.add_argument(
        '--sleep-time',
        action="store",
        dest='sleep',
        type=int)
    args = parser.parse_args()
    main(args.val, args.sleep)
