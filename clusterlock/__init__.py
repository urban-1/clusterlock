import logging as lg
import traceback
import threading
import time

from sqlalchemy import Column, Integer, String, Boolean, UniqueConstraint, Sequence, Text, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import relationship, scoped_session, sessionmaker, class_mapper
from sqlalchemy.ext.declarative import declarative_base



import setpath
import os
import socket

__all__ = []

def get_who():
    return "%s:%d:%d" % (socket.getfqdn(), os.getpid(), threading.currentThread().ident)

class ClusterLockError(Exception):
    pass

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
    events = relationship("LockEvent", cascade="all, delete-orphan")

    

class LockEvent(Base):
    __tablename__ = 'cluster_lock_event'
    __table_args__ = (
        PrimaryKeyConstraint('context_id', 'started', 'who'),
    )
    
    #id = Column(Integer, Sequence('seq_cluster_lock_evt_id'), primary_key=True, autoincrement=True)
    context_id = Column(Integer, ForeignKey('cluster_lock_ctx.id'))
    started = Column(Integer)                  # When this started, epoch
    ended = Column(Integer, default=0)         # When this ended, epoch
    who = Column(String(512))                  # Who locked it last
    
    

class ClusterLockBase(object):

    def __init__(self, engine, session, what, context="-", min_bound=0, max_bound=1, value=1, sleep_interval=0.1):
        """
        Initialize the instance with a given session and engine
        """
        self._engine = engine
        self._session = session
        self._what = what
        self._context = context
        self._min_bound = min_bound
        self._max_bound = max_bound
        self._time_slept = 0
        self._sleep_interval = sleep_interval
        self._events = []
        
        # Create schema
        try:
            Base.metadata.create_all(self._engine)
        except:
            pass
        
        Base.query = self._session.query_property()
        
        try:
            # Create a LockContext for this context
            ll = LockContext(
                what=what,
                count=value,
                context = context)
            
            self._session.add(ll)
            self._session.commit()
        except:
            self._session.rollback()
        
    
    def __enter__(self):
        self.aquire()
        
    def __exit__(self, type, value, traceback):
        self.release()
    
    def aquire(self, max_wait=0):
        """
        Lock an entry. This should match the ticket_number and current status while 
        the status should not be locked
        """
        rc = 0
        who = get_who()
        self._time_slept = 0
        
        while rc == 0:
            lg.debug("Trying")
            try:
                rc = LockContext.query.filter(LockContext.what==self._what)\
                                .filter(LockContext.context==self._context)\
                                .filter(LockContext.count > self._min_bound).update({"count": LockContext.count - 1})
                
                lg.debug("Acquire lock resulted in %d rows affected" % rc)
                self._session.commit()
            except:
                self._session.rollback()
                print(traceback.format_exc())
                lg.debug(traceback.format_exc())
            
            if rc == 0:
                if max_wait > 0 and self._time_slept >= max_wait:
                    raise ClusterLockError("Max time used... failed to acquire")
                
                time.sleep(0.1)
                self._time_slept += self._sleep_interval
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
        
        try:
            
            ## 2nd way
            rc = LockContext.query.filter(LockContext.what==self._what)\
                                .filter(LockContext.context==self._context)\
                                .filter(LockContext.count < self._max_bound).update({"count": LockContext.count + 1})
            
            lg.debug("Release lock resulted in %d rows affected" % rc)
            self._session.commit()
            
            # Delete our event...
            if len(self._events):
                self._session.delete(self._events[-1])
                self._session.commit()
        except:
            self._session.rollback()
            print(traceback.format_exc())
            lg.debug(traceback.format_exc())
            
        if rc != 1:
            raise ClusterLockReleaseError("Number of releases mismatch! Someone consumes more!")


class Lock(ClusterLockBase):
    
    
    def __init__(self, engine, session, what, context="-"):
        """
        Limited constructor
        """
        super(Lock, self).__init__(engine, session, what, context, 0, 1, 1)
        
        
    def reset(self):
        """
        Only reset's to 1!
        """
        super(Lock, self).reset(1)


class Semaphore(ClusterLockBase):
    pass

