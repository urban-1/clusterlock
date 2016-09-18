#!/usr/bin/env python

import sys
import time
import threading
import logging as lg
from random import uniform

import setpath
import clusterlock as cl


if len(sys.argv) < 2:
    print("Ahhmm i need a job name as 1st argument")
    exit()

lg.basicConfig(level=lg.INFO)

cl.init_db(*cl.get_backend("config.json"))

# Create a new lock for a specific device under the domain 
# light-levels
lock = cl.Lock("device", "light-levels", sleep_interval=1)
name = sys.argv[1]


while [ True ]:
    print("%10s: Waiting" % name)
    with lock:
        print("%10s: Got it!" % name)
        time.sleep(uniform(9, 15.5))
    
    print("%10s: Released" % name)
    # Give a chance to others
    time.sleep(0.5)

