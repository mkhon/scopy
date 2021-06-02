#!/usr/bin/env python
from __future__ import print_function
import getopt
import io
import json
import os
import re
import subprocess
import sys
import time

def get_disks():
    lsblk_json = subprocess.check_output(["lsblk", "-JOd"])
    return json.loads(lsblk_json)["blockdevices"]

def find_disk(disk):
    if re.match(r"/dev/", disk):
        return disk

    notified = False
    while True:
        for d in get_disks():
            if d["model"] == disk:
                return "/dev/{}".format(d["name"])
        if not notified:
            print("SCOPY: Waiting for device {} to appear...".format(disk))
            notified = True
        time.sleep(1)

def list_disks():
    for d in get_disks():
        print("/dev/{}: {}".format(d["name"], d["model"]))

def each_chunk(stream, separator):
    buffer = b''
    while True:
        chunk = stream.read(1)
        if not chunk:
            # EOF
            yield buffer.decode("utf-8")
            break
        buffer += chunk
        if separator.find(chunk) >= 0:
            yield buffer.decode("utf-8")
            buffer = b''

def run_dd(source_disk, target_disk, offset, limit):
    print("SCOPY: Using offset {}, limit {}".format(offset, limit))
    s, t, exitcode = None, None, None
    while True:
        # find target
        new_t = find_disk(target_disk)
        if new_t != t:
            t = new_t
            print("SCOPY: Using target device {} ({})".format(t, target_disk))

        # find source
        new_s = find_disk(source_disk)
        if new_s != s:
            s = new_s
            print("SCOPY: Using source device {} ({})".format(s, source_disk))

        # there was an error: zero out target and advance
        if exitcode is not None:
            args = ["dd", "if=/dev/zero", "of={}".format(t), "bs=512", "status=none", "count=1", "seek={}".format(offset)]
            print("EXEC: {}".format(" ".join(args)))
            subprocess.check_call(args)
            offset += 1
            if limit is not None:
                limit -= 1
            print("SCOPY: New offset {}, limit {}".format(offset, limit))

        # do sector copy
        args = ["dd", "if={}".format(s), "of={}".format(t), "bs=512", "status=progress", "skip={}".format(offset), "seek={}".format(offset)]
        if limit is not None:
            args += ["count={}".format(limit)]
        print("EXEC: {}".format(" ".join(args)))
        proc = subprocess.Popen(args, stderr=subprocess.PIPE)
        sectors_read = None
        for line in each_chunk(proc.stderr, b"\r\n"):
            print(line, end='', file=sys.stderr)
            m = re.match(r"(\d+)\+\d+ records in", line)
            if m:
                sectors_read = int(m.group(1))
                print("SCOPY: {} sectors read".format(sectors_read))
        exitcode = proc.wait()
        if exitcode == 0:
            # we are done!
            break

        if sectors_read is not None:
            offset += sectors_read
            if limit is not None:
                limit -= sectors_read
            print("SCOPY: New offset {}, limit {}, exit code {}".format(offset, limit, exitcode))
        else:
            print("SCOPY: Failed to determine number of sectors read, exit code {}".format(exitcode))

def usage(exitcode):
    print("Usage: {} [-h] [-o offset] [-l limit] source-disk target-disk".format(os.path.basename(sys.argv[0])))
    sys.exit(exitcode)

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hLl:o:", ["help", "limit=", "offset="])
    except getopt.GetoptError as err:
        print(err)
        usage(2)

    limit = None
    offset = 0
    for o, a in opts:
        if o in ("-h", "--help"):
            usage(0)
        if o in ("-L", "--llist"):
            list_disks()
            sys.exit(0)

        if o in ("-l", "--limit"):
            limit = int(a)
        elif o in ("-o", "--offset"):
            offset = int(a)
        else:
            assert False, "unhandled option"

    if len(args) < 2:
        usage(0)

    source_disk, target_disk = args
    run_dd(source_disk, target_disk, offset, limit)

if __name__ == "__main__":
    try:
        sys.stdout = io.TextIOWrapper(open(sys.stdout.fileno(), 'wb', 0), write_through=True)
    except TypeError:
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    main()
