#!/usr/bin/env python

import sys
import time
import threading
import logging as lg
from random import uniform, randint

import setpath

from clusterlock import Lock, get_backend, acquire_all

lg.basicConfig(level=lg.INFO)

if len(sys.argv) < 2:
    print("Ahhmm i need a job name as 1st argument")
    exit()
    
name = sys.argv[1]


engine, session = get_backend("config.json")

# Create a new lock for a specific device under the domain 
# light-levels
locks = [Lock(engine, session, "device", "light-levels", sleep_interval=1),
         Lock(engine, session, "device", "irrelevant", sleep_interval=1),
         Lock(engine, session, "device", "apsou", sleep_interval=1)]

while [ True ]:
    l1 = randint(0, len(locks)-1)
    l2 = l1
    while l2 == l1:
        l2 = randint(0, len(locks)-1)
    
    print("%10s: Waiting for %d,%d" % (name, l1, l2))
    acquire_all([locks[l1], locks[l2]], max_wait_per_lock=0.5, total_wait=None)
    
    print("%10s: GOT THEM ALL" % name)
    time.sleep(uniform(3, 5))
    
    print("%10s: Released" % name)
    locks[l1].release()
    locks[l2].release()
    # Give a chance to others
    time.sleep(1)

