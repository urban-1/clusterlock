#!/usr/bin/env python

import sys
import time
import threading
import logging as lg
from random import uniform

import setpath

from clusterlock import Semaphore, ClusterLockReleaseError, ClusterLockError, get_backend

if len(sys.argv) < 2:
    print("Ahhmm i need a job name as 1st argument")
    exit()
    
lg.basicConfig(level=lg.INFO)

engine, session = get_backend("config.json")

# Create a new lock for a specific device under the domain 
# light-levels
lock = Semaphore(engine, session, "device", "something-else", max_bound=5, duration=2, cleanup_every=3)
name = sys.argv[1]

while [ True ]:
    print("%10s: Waiting" % name)
    try:
        lock.acquire(max_wait=5)
    except ClusterLockError as e:
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

