#!/usr/bin/env python

import sys
import time
import json
import threading
from sqlalchemy import create_engine
from sqlalchemy.orm import  scoped_session, sessionmaker
from base64 import b64decode
import logging as lg
from random import uniform

import setpath

from clusterlock import Semaphore

#url = 'sqlite:///test.sqlite'

lg.basicConfig(level=lg.INFO)

with open("config.json") as f:
    cfg = json.loads(f.read())

# Build connection URL
url = "%s%s:%s@%s/?service_name=%s" % (cfg['proto'],
                                    cfg['user'],
                                    b64decode(cfg['pass']),
                                    cfg['host'],
                                    cfg['service_name'])
engine = create_engine(url)
session = scoped_session(sessionmaker(bind=engine))

# Create a new lock for a specific device under the domain 
# light-levels
lock = Semaphore(engine, session, "device", "something-else", 0, 5, value=5)
name = sys.argv[1]

while [ True ]:
    print("%10s: Waiting" % name)
    with lock:
        print("%10s: Got it!" % name)
        time.sleep(uniform(0.5, 5.5))
    
    print("%10s: Released" % name)
    time.sleep(0.1)

