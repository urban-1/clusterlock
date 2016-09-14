#!/usr/bin/env python

import sys
import time
import threading
import logging as lg
from random import uniform

import setpath

from clusterlock import Semaphore, ClusterLockReleaseError, get_backend

if len(sys.argv) < 2:
    print("Ahhmm i need a job name as 1st argument")
    exit()
    
lg.basicConfig(level=lg.INFO)

engine, session = get_backend("config.json")

# Create a new lock for a specific device under the domain 
# light-levels
lock = Semaphore(engine, session, "device", "something-else", value=5, duration=2, max_bound=5, min_bound=0, cleanup_every=1)
name = sys.argv[1]

while [ True ]:
    print("%10s: Waiting" % name)
    try:
        lock.aquire(max_wait=5)
    except Exception as e:
        print(e.message)
        continue
    
    print("%10s: Got it!" % name)
    time.sleep(uniform(0.5, 5.5))
    try:
        lock.release()
    except ClusterLockReleaseError as e:
        pass
    print("%10s: Released" % name)
    time.sleep(0.1)

