#!/usr/bin/env python
import json
import os
import sys
import daemon
import subprocess
import socket
import random

commands = []


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print "Usage:\n ",sys.argv[0], "use configuration file"
        print "For Example:"
        print " %s channels.cfg"%(sys.argv[0])
        sys.exit(0);
    cfg = sys.argv[1]
    data = ""
    with open(cfg, "r") as files:
        lines = files.readlines()
        data = "".join(lines)
    all_config = json.loads(data)
    print all_config["udp"]
    for channel in all_config["channels"]:
        dest  = channel['dest']
        number = channel['program_number'] 
        port = random.randint(50000, 60000)
        args = ["udp2rtmp", "-i", port, "-y", dest,  "-p", str(number)]
        commands.append(args)
    for chan in commands:
        print chan
        subprocess.call(chan)
    while True:
        pass

