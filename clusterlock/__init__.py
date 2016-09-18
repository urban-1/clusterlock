import logging as lg
import traceback
import threading
import time
import json
from base64 import b64decode
from multiprocessing import Lock as mLock
import random
import signal
import sys


from sqlalchemy import Column, Integer, String, Boolean, UniqueConstraint, Sequence, Text, ForeignKey, PrimaryKeyConstraint, create_engine
from sqlalchemy.orm import relationship, scoped_session, sessionmaker, class_mapper
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.reflection import Inspector



import setpath
import os
import socket


class ClusterLockError(Exception):
    """
    We failed to lock something. Thrown when a ``max_wait`` has been exhausted
    """
    pass


class ClusterLockReleaseError(Exception):
    """
    This is usually expected after a `ClusterCleanUp` event. If us or anyone else
    cleaned our lock forsefully but our process was still active, this error
    will be thrown when the process eventually tries to release the lock
    """
    pass


class ClusterCleanUpError(Exception):
    pass


CREATE_LOCK = mLock()
"""Host local create tables lock"""

CLEAN_LOCKS = {}
"""One cleaner per thread, no more, no less"""

CLEAN_DURATION = 30
CLEAN_MAX_TIME = 60
CLEAN_FAIL_MSG = ("Failed to acquire 'clean-lock' which means that:\n\n"
                  "  (1) either the db connection is not there or\n"
                  "  (2) indication of extrem request on a resource or\n"
                  "  (3) there is a locked cleaner entry you have to manually unlock.\n\n"
                  "Cleaning the cleaning lock has already failed... Nothing else I can do")
"""The error message when we cannot clean the cleaner!"""

GLOBAL_LOCKS = {}

BACKENDS = {}


def init_db(engine, session):
    """
    Initialize the module, this should be called in every thread!
    """
    tid = get_who()
    if tid not in BACKENDS.keys():
        BACKENDS[tid] = (engine, session)
    
    if len(BACKENDS) == 1:
        with CREATE_LOCK:
            try:
                lg.info("Creating schema if needed")
                Base.metadata.create_all(bind=engine)
            except:
                pass
        
def get_db():
    """
    Returns a tuple : engine, session for this thread
    """
    tid = get_who()
    if tid not in BACKENDS.keys():
        raise RuntimeError("You must call init() in every thread")
    return BACKENDS[tid]
    

def get_backend(cfgpath):
    """
    Helper function to get an engine and a session to the database given a
    configuration file. For the format of this file look into `tests/config.json.sample`
    """
    with open(cfgpath, "r") as f:
        cfg = json.loads(f.read())
    
    if cfg["mode"] == "oracle":
        o = cfg["oracle"]
        url = "%s%s:%s@%s/?service_name=%s" % \
            (o['proto'], o['user'], b64decode(o['pass']), o['host'], o['service_name'])
    elif cfg["mode"] == "mysql":
        m = cfg["mysql"]
        url = "%s%s:%s@%s/%s" % \
            (m['proto'], m['user'], b64decode(m['pass']).decode(), m['host'], m['database'])
    
    engine = create_engine(url)
    session = scoped_session(sessionmaker(bind=engine))
    
    return engine, session

def get_who():
    """
    Return a cluster/globally unique identification. This is in the format:
    
        <hostname>:<process-id>:<thread-id>
    
    """
    return "%s:%d:%d" % (socket.getfqdn(), os.getpid(), threading.currentThread().ident)


def get_clean_lock():
    
    # Get a unique thread-safe ID
    tid = get_who()
    
    if tid not in CLEAN_LOCKS.keys():
        CLEAN_LOCKS[tid] = Lock("db", "clean-lock", 
                            duration=CLEAN_DURATION,      # Cleaner should not take more than ... 
                            cleanup_every=CLEAN_MAX_TIME  # Do not clean before the cleaner times out...
                            )
        CLEAN_LOCKS[tid].__attempted = False
    
    return CLEAN_LOCKS[tid]

def get_global_lock():
    
    # Get a unique thread-safe ID
    tid = get_who()
    
    if tid not in GLOBAL_LOCKS.keys():
        GLOBAL_LOCKS[tid] = Lock("db", "global-lock", 
                            duration=10,
                            cleanup_every=10
                            )
    
    return GLOBAL_LOCKS[tid]
    
def acquire_all(locks, max_wait_per_lock=1, sleep_time=.5, total_wait=None):
    """
    Acquire all locks or none... This should avoid deadlocks
    """
    #locks = locks[:]
    started = time.time()
    
    while [ True ]:
        
        # Lock global
        with locks[0].get_global_lock():
            lg.debug("Got global lock")
            # Try to lock all
            gotThem = True
            for l in range(0, len(locks)):
                try:
                    lg.debug("Trying lock %d:%s" % (l, locks[l]._tag))
                    locks[l].acquire(max_wait_per_lock)
                    lg.debug("Got lock %d:%s" % (l, locks[l]._tag))
                except ClusterLockError as e:
                    gotThem = False
                    lg.debug("Failed at %d" % l)
                    if l != 0:
                        for l2 in range(0, l):
                            lg.debug("Releasing lock %d:%s" % (l2, locks[l2]._tag))
                            locks[l2].release()
                    
                    # Do not attempt any more locks
                    break
                
    
        # Check what we did
        if gotThem:
            return
        
        if total_wait is not None and time.time() - started > total_wait:
            raise ClusterLockError("Acquire all timedout")
        
        time.sleep(sleep_time)


class CustomBase(object):
    """
    A simple base I tend to use... It provides helper functions which will be
    available to all the ORM classes
    """
    def toDict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    def fromDict(self, d):
        for c in self.__table__.columns:
            if c.name in d.keys():
                setattr(self, c.name, d[c.name])


Base = declarative_base(cls=CustomBase)
"""Declarative base using our CustomBase class"""


class LockContext(Base):
    """
    The ``cluster_lock_ctx`` table replresentation. This table specifies the
    available locks/semaphores
    """
    __tablename__ = 'cluster_lock_ctx'
    __table_args__ = (
        UniqueConstraint('what', 'context', name='_LockContext_uniq'),
        {'mysql_engine':'MEMORY'}
    )
    
    id = Column(Integer, Sequence('seq_cluster_lock_ctx_id'), primary_key=True, autoincrement=True)
    what = Column(String(512))       # What do we lock
    count = Column(Integer)          # Count for semaphores
    context = Column(String(512))    # Which software context (application)
    duration = Column(Integer, default=-1)       # Average job duration (used to identify dead locks)
    events = relationship("LockEvent", cascade="save-update, merge, delete")
    
    def __str__(self):
        return "what=%s, ctx=%s, c=%d" % (self.what, self.context, self.count)
    

class LockEvent(Base):
    """
    The ``cluster_lock_evt`` table replresentation. Keeps acquire/release entries.
    At the moment we store only acquire events which is used to detect dead entries
    and locks. Release will delete the entry
    """
    __tablename__ = 'cluster_lock_evt'
    __table_args__ = (
        PrimaryKeyConstraint('context_id', 'started', 'who'),
        {'mysql_engine':'MEMORY'}
    )
    
    context_id = Column(Integer, ForeignKey('cluster_lock_ctx.id'), autoincrement=False)
    started = Column(Integer, autoincrement=False)                  # When this started, epoch
    ended = Column(Integer, default=0, autoincrement=False)         # When this ended, epoch
    who = Column(String(512))                                       # Who locked it last
    
    def __str__(self):
        return "ctx=%d, who=%s, s=%d" % (self.context_id, self.who, self.started)
    


class ClusterLockBase(object):
    """
    The base implementation of the database locking mechanisms. Both Locks and
    Semaphores inherit this class. This class has the full functionality of
    these objects, while the Lock just constrains it by overriding some methods
    """
    
    def __init__(self, what, context="-", max_bound=1, value=-1, duration=-1, sleep_interval=0.5, cleanup_every=10):
        """
        Initialize the instance with a given session and engine
        """
        self._engine, self._session = get_db()
        self._what = what
        self._context = context
        self._min_bound = 0
        self._max_bound = max_bound
        self._cleanup_every = cleanup_every
        self._time_slept = 0
        self._sleep_interval = sleep_interval
        self._events = []
        self._tag = "%s:%s" % (self._what, self._context)
        self._duration = duration
        self.__is_context_manager = False
        
        if value == -1:
            value = max_bound
        
        Base.query = self._session.query_property()
        
        try:
            # try to get it first (preserves auto increment IDs from failed
            # insert attempts...)
            rc = LockContext.query.filter(LockContext.what==self._what)\
                                .filter(LockContext.context==self._context)
            try:
                rc.one()
            except:
                # Create a LockContext for this context - it does not exist
                ll = LockContext(
                    what=what,
                    count=value,
                    context=context,
                    duration=duration)
                
                self._session.add(ll)
                self._session.commit()
        except:
            self._session.rollback()
    
    def __enter__(self):
        self.__is_context_manager = True
        self.acquire()
        
    def __exit__(self, type, value, traceback):
        self.release()
        self.__is_context_manager = False
            
    def __handle_cleanup(self):
        """
        This is a function only because it was long and complex...
        """
        #lg.debug("CLEAN %s: %.2f >= %.2f" % (self._tag, self._time_slept, self._cleanup_every))
        lg.debug("%s: Cleaning up" % self._tag)
        
        cleanLock = get_clean_lock()
        # Handle cleaning the cleaner!
        if self._tag == "db:clean-lock":
            lg.info("Cleaning the cleaner (did this already? = %s)" % str(cleanLock.__attempted))
            # Try only once...
            if cleanLock.__attempted:
                raise ClusterCleanUpError(CLEAN_FAIL_MSG)
            
            # Mark that we have tried that!
            cleanLock.__attempted = True
            
        # Aquire cleaning lock ... wait 30 seconds... if this fails
        # stop with the appropriate error message
        try:
            cleanLock.acquire(max_wait=CLEAN_MAX_TIME)
        except ClusterLockError:
            raise ClusterCleanUpError(CLEAN_FAIL_MSG)
        
        
        lg.debug("Got Cleaning up LOCK")
        now = int(time.time())
        expired = self._session.query(LockEvent, LockContext)\
            .filter(LockEvent.context_id == LockContext.id)\
            .filter(LockContext.what == self._what)\
            .filter(LockContext.context == self._context)\
            .filter(LockContext.duration > 0)\
            .filter(now - LockEvent.started > LockContext.duration)\
            .all()
        
        cleaned = False
        
        # Loop and clean!
        # Another process or thread might clean these too... since they are
        # not necessarily ours
        for entry in expired:
            cid = entry[1].id
            tmpmsg = "Cleaning Context ID=%d, who=%s, started=%d" % (cid, entry[0].who, entry[0].started)
            lg.info(tmpmsg)
            # Remove lock event
            self._session.delete(entry[0])
            # Add one to the pool
            entry[1].count += 1
            # Done
            self._session.commit()
            cleaned = True
        
        # release the lock
        cleanLock.release()
        
        # If we did something
        if cleaned:
            if self._tag == "db:clean-lock":
                cleanLock.__attempted = False
        

    
    def acquire(self, max_wait=0):
        """
        Acquire.
        """
        lg.debug("Aquire %s" % self._tag)
        rc = 0
        who = get_who()
        self._started = time.time()
        
        while rc == 0:
            #lg.debug("Trying")
            try:
                rc = LockContext.query.filter(LockContext.what == self._what)\
                                .filter(LockContext.context == self._context)\
                                .filter(LockContext.count > self._min_bound).update({"count": LockContext.count - 1})
                
                #lg.debug("Acquire lock resulted in %d rows affected" % rc)
                self._session.commit()
            except:
                self._session.rollback()
                print(traceback.format_exc())
                lg.debug(traceback.format_exc())
            
            if rc == 0:
                
                # Sleep as for a while
                time.sleep(self._sleep_interval)
                time_slept = time.time() - self._started
                
                # Handle max_wait here since we failed to acquire
                #lg.debug("MAX %s: %.2f >= %.2f" % (self._tag, time_slept, max_wait))
                if max_wait > 0 and time_slept >= max_wait:
                    raise ClusterLockError("Max time used... failed to acquire")
                
                # Handle cleaning
                # No point if:
                #  - cleaning is disabled
                #  - duration is unknown!
                if self._cleanup_every > 0 and self._duration > 0 and time_slept > self._cleanup_every:
                    self.__handle_cleanup()
                    
            else:
                # Create our event...
                ctx = LockContext.query.filter(LockContext.what==self._what)\
                                       .filter(LockContext.context ==self._context ).one()
                
                self._events.append(LockEvent(
                        context_id=ctx.id,
                        started=int(time.time()),
                        who = who
                        ))
                
                self._session.add(self._events[-1])
                self._session.commit()
                
        
    def release(self):
        """
        Release
        """
        lg.debug("Release %s" % self._tag)
        who = get_who()
        rc = 0
        
        # Delete our event...
        if len(self._events):
            try:
                # It is safe to do this since Lock instances should not be shared
                # amongst threads
                self._session.delete(self._events.pop())
                
                # Reduce count ONLY of the above succeeds...
                rc = LockContext.query.filter(LockContext.what==self._what)\
                                      .filter(LockContext.context==self._context)\
                                      .filter(LockContext.count < self._max_bound).update({"count": LockContext.count + 1})
                
                #lg.debug("Release lock resulted in %d rows affected" % rc)
                
                self._session.commit()
            except:
                self._session.rollback()
                lg.debug("... Timedout? Someone cleaned for us...(%s)" % self._tag)
                #lg.debug(traceback.format_exc())

            
        if rc != 1:
            raise ClusterLockReleaseError("Number of releases mismatch! Someone consumes more! (%s)" % self._tag)
            
            
    
class Lock(ClusterLockBase):
    """
    The Lock class allows for only one lock at a time
    """
    
    def __init__(self, what, context="-", **kw):
        """
        Limited constructor
        """
        super(Lock, self).__init__(what, context, value=1, max_bound=1, **kw)
        
        
    def reset(self):
        """
        Only reset's to 1!
        """
        super(Lock, self).reset(1)

        
class Semaphore(ClusterLockBase):
    pass


