import logging as lg
import traceback
import threading
import time
import json
from base64 import b64decode


from sqlalchemy import Column, Integer, String, Boolean, UniqueConstraint, Sequence, Text, ForeignKey, PrimaryKeyConstraint, create_engine
from sqlalchemy.orm import relationship, scoped_session, sessionmaker, class_mapper
from sqlalchemy.ext.declarative import declarative_base



import setpath
import os
import socket


class ClusterCleanUp(Exception):
    """
    Used as an event to notify you that something was cleaned up. You can find
    the context (ctx) and event (evt) attached
    """
    def __init__(self, message, ctx=None, evt=None):
        super(ClusterCleanUp, self).__init__(message)
        self.ctx=ctx
        self.evt=evt


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


CLEAN_LOCKS = {}
"""One cleaner per thread, no more, no less"""
CLEAN_FAIL_MSG = ("Failed to acquire 'clean-lock' which means that either the "
                  "db connection is not there or there is a locked entry "
                  "which you have to manually unlock. Cleaning the cleaning lock "
                  "has already failed... Nothing else I can do")

def get_backend(cfgpath):
    with open(cfgpath, "r") as f:
        cfg = json.loads(f.read())
    
    if cfg["mode"] == "oracle":
        o = cfg["oracle"]
        url = "%s%s:%s@%s/?service_name=%s" % \
            (o['proto'], o['user'], b64decode(o['pass']), o['host'], o['service_name'])
    elif cfg["mode"] == "mysql":
        m = cfg["mysql"]
        url = "%s%s:%s@%s/%s" % \
            (m['proto'], m['user'], b64decode(m['pass']), m['host'], m['database'])
    
    engine = create_engine(url)
    session = scoped_session(sessionmaker(bind=engine))
    
    return engine, session

def get_who():
    return "%s:%d:%d" % (socket.getfqdn(), os.getpid(), threading.currentThread().ident)


class CustomBase(object):
    
    def toDict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    def fromDict(self, d):
        for c in self.__table__.columns:
            if c.name in d.keys():
                setattr(self, c.name, d[c.name])


Base = declarative_base(cls=CustomBase)


class LockContext(Base):
    __tablename__ = 'cluster_lock_ctx'
    __table_args__ = (UniqueConstraint('what', 'context', name='_LockContext_uniq'),)
    
    id = Column(Integer, Sequence('seq_cluster_lock_ctx_id'), primary_key=True, autoincrement=True)
    what = Column(String(512))       # What do we lock
    count = Column(Integer)          # Count for semaphores
    context = Column(String(512))    # Which software context (application)
    duration = Column(Integer, default=-1)       # Average job duration (used to identify dead locks)
    events = relationship("LockEvent", cascade="save-update, merge, delete")

    

class LockEvent(Base):
    __tablename__ = 'cluster_lock_evt'
    __table_args__ = (
        PrimaryKeyConstraint('context_id', 'started', 'who'),
    )
    
    context_id = Column(Integer, ForeignKey('cluster_lock_ctx.id'), autoincrement=False)
    started = Column(Integer, autoincrement=False)                  # When this started, epoch
    ended = Column(Integer, default=0, autoincrement=False)         # When this ended, epoch
    who = Column(String(512))                                       # Who locked it last
    
    

class ClusterLockBase(object):

    def __init__(self, engine, session, what, context="-", value=1, duration=-1, max_bound=1, min_bound=0, sleep_interval=0.1, cleanup_every=10):
        """
        Initialize the instance with a given session and engine
        """
        self._engine = engine
        self._session = session
        self._what = what
        self._context = context
        self._min_bound = min_bound
        self._max_bound = max_bound
        self._cleanup_every = cleanup_every
        self._time_slept = 0
        self._sleep_interval = sleep_interval
        self._events = []
        self._tag = "%s:%s" % (self._what, self._context)
        
        # Create schema only if required
        if not engine.dialect.has_table(engine, "cluster_lock_ctx"): 
            Base.metadata.create_all(self._engine)
        
        
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
                print("INSERT")
                self._session.commit()
        except:
            self._session.rollback()
            
        
    
    def __enter__(self):
        self.aquire()
        
    def __exit__(self, type, value, traceback):
        self.release()
        
    def __handle_cleanup(self):
        """
        This is a function only because it was long and complex...
        """
        #lg.debug("CLEAN %s: %.2f >= %.2f" % (self._tag, self._time_slept, self._cleanup_every))
        lg.debug("%s: Cleaning up" % self._tag)
        
        cleanLock = self.get_clean_lock()
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
            cleanLock.aquire(max_wait=10) # TODO: Module level Parameter
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
        for entry in expired:
            cid = entry[1].id
            tmpmsg = "Cleaning Context ID=%d, who=%s, started=%d" % (cid, entry[0].who, entry[0].started)
            lg.debug(tmpmsg)
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
            raise ClusterCleanUp(tmpmsg, entry[1], entry[0])
        
    def get_clean_lock(self):
        global CLEAN_LOCKS
        
        # Get a unique thread-safe ID
        tid = get_who()
        
        if tid not in CLEAN_LOCKS.keys():
            CLEAN_LOCKS[tid] = Lock(self._engine, self._session, "db", "clean-lock", 
                                duration=30,      # Cleaner should not take more than ... TODO: Module level Parameter
                                cleanup_every=10  # TODO: Module level Parameter
                                )
            CLEAN_LOCKS[tid].__attempted = False
        
        return CLEAN_LOCKS[tid]
    
    def aquire(self, max_wait=0):
        lg.debug("Aquire %s" % self._tag)
        """
        Lock an entry. This should match the ticket_number and current status while 
        the status should not be locked
        """
        rc = 0
        who = get_who()
        self._time_slept = 0
        
        while rc == 0:
            #lg.debug("Trying")
            try:
                rc = LockContext.query.filter(LockContext.what==self._what)\
                                .filter(LockContext.context==self._context)\
                                .filter(LockContext.count > self._min_bound).update({"count": LockContext.count - 1})
                
                #lg.debug("Acquire lock resulted in %d rows affected" % rc)
                self._session.commit()
            except:
                self._session.rollback()
                print(traceback.format_exc())
                lg.debug(traceback.format_exc())
            
            if rc == 0:
                
                # Sleep as for a while
                time.sleep(0.1)
                self._time_slept += self._sleep_interval
                
                # Handle max_time here since we failed to aquire
                #lg.debug("MAX %s: %.2f >= %.2f" % (self._tag, self._time_slept, max_wait))
                if max_wait > 0 and self._time_slept >= max_wait:
                    raise ClusterLockError("Max time used... failed to acquire")
                
                # Handle cleaning
                if self._cleanup_every > 0 and self._time_slept > self._cleanup_every:
                    self.__handle_cleanup()
                    
            else:
                # Create our event...
                ctx = LockContext.query.filter(LockContext.what==self._what and \
                                               LockContext.context==self._context).one()
                self._events.append(LockEvent(
                        context_id=ctx.id,
                        started=int(time.time()),
                        who = who
                        ))
                
                self._session.add(self._events[-1])
                self._session.commit()
                
        
    def release(self):
        """
        Lock an entry. This should match the ticket_number and current status while 
        the status should not be locked
        """
        who = get_who()
        rc = 0
        
        # Delete our event...
        if len(self._events):
            try:
                self._session.delete(self._events.pop())
                
                # Reduce count ONLY of the above succeeds...
                rc = LockContext.query.filter(LockContext.what==self._what)\
                                    .filter(LockContext.context==self._context)\
                                    .filter(LockContext.count < self._max_bound).update({"count": LockContext.count + 1})
                
                #lg.debug("Release lock resulted in %d rows affected" % rc)
                
                self._session.commit()
            except:
                self._session.rollback()
                lg.debug("... Timedout? Someone cleaned for us...")
                #lg.debug(traceback.format_exc())

            
        if rc != 1:
            raise ClusterLockReleaseError("Number of releases mismatch! Someone consumes more!")

    
class Lock(ClusterLockBase):
    
    
    def __init__(self, engine, session, what, context="-", **kw):
        """
        Limited constructor
        """
        super(Lock, self).__init__(engine, session, what, context, **kw)
        
        
    def reset(self):
        """
        Only reset's to 1!
        """
        super(Lock, self).reset(1)

        
class Semaphore(ClusterLockBase):
    pass

