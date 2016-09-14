#!/usr/bin/env python

import sys
import time
import threading
import logging as lg
from random import uniform

import setpath

from clusterlock import Lock, get_backend

if len(sys.argv) < 2:
    print("Ahhmm i need a job name as 1st argument")
    exit()

lg.basicConfig(level=lg.DEBUG)

engine, session = get_backend("config.json")

# Create a new lock for a specific device under the domain 
# light-levels
lock = Lock(engine, session, "device", "light-levels")
name = sys.argv[1]

while [ True ]:
    print("%10s: Waiting" % name)
    with lock:
        print("%10s: Got it!" % name)
        time.sleep(uniform(9, 15.5))
    
    print("%10s: Released" % name)
    time.sleep(0.1)

