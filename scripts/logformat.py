#!/usr/local/bin/python

import sys
import json

while True:
    line = sys.stdin.readline()
    try:
        fields = json.loads(line)
    except Exception:
        print(line)
        continue

    msg = fields.get("msg")
    time = fields.get("time")
    if msg and time:
        print("%s %s" % (time, msg))
    else:
        print(line)
    sys.stdout.flush()
