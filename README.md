# clusterlock

A database based locking and semaphore implementation

## What is `clusterlock`?

Any distributed system will most probably require a cluster-global locking 
mechanism at some point. This can ensure that two worker nodes do not process the
same request, or that a specific resource is not being overwhelmed by multiple
processing hosts.

This module was developed having in mind handling network resources in a distributed
manner and thus we will use this as our main example to explain the API.

Think of a scenario where multiple users interact with network devices from 
different hosts. Although our devices have decoupled data and control planes,
the control plane gets overwhelmed and "laggy" when multiple SNMP queries are
happening simultaneously. In this case we could say:

    "Ensure that maximum three SNMP queries happen against a network device"
    
The parameters we require to implement the above constraint are:

1.  The target device: `what` we try to lock
1.  The maximum number of simultaneous queries: `max_bound`

Continuing our scenario, we also have software that performs unattended
configuration backup. Generating configuration can be "heavy" on the CPU and
internal storage - some devices don't really like it. We also like to constraint
it to:

    "Only one process can generate config on a device at any time"
    
As you can see, the two different functions we want to perform, apply on the same
device but have different parameters and should not conflict each-other.
Therefore, we introduce the `context` parameter which allows as to define
different functions on the same target ... and that is (more or less) our
API parameters and definition! 

## Getting started

`clusterlock` provides two synchronization classes that derive from the same base: 
`Semaphore` and `Lock`. The `Lock` is a "short-cut" to a `Semaphore` configured for
maximum one process at any given time.

To create a lock we need a SQLAlchemy `engine` and `session`, clusterlock will 
do the rest (including creating tables if not there):

    lock = Lock(engine, session, "device", "snmp-query")
    
To acquire the lock you can either use:

    lock.acquire()
    # ... long job ...
    lock.release()
    
or use pythons' context manager:

    with lock:
        # ... long job ...
        
    # The lock is auto-magically release
    
    
Optionally, you can set the maximum time you want to wait for the lock in which 
case the lock will raise a `ClusterLockError` on time-out. This is in seconds:

    lock.acquire(max_wait=5)  # or just ...
    lock.acquire(5)

    
### Fine tuning

There are few other options which are particularly useful for `Semaphore`'s. 
These are:
                     
-  `max_bound`: The maximum number this Semaphore can be acquired
-  `value`: The current value of the semaphore on creation. This should be
    equal to `max_bound` (leave empty and it will be set to `max_bound`)
-   `duration`: How long does this job take. This is used by the cleaning 
    function later. Default is -1 (disabled)
-   `sleep_interval`: How long do we sleep between trying to acquire the lock.
    This defaults to roughly 100ms and you should increase it if you want to 
    preserve CPU.

Example:

    lock = Semaphore(engine, session, "device", "get-config",\
                     value=5, max_bound=5, duration=2, sleep_interval=0.1, cleanup_every=3)

                     
### Cleaning up

Processes tend to die... and this leaves locked context. To tackle it we 
introduce the cleaning up function. This runs in lazy mode and looks for dead 
entries while we are waiting to acquire. A dead entry is a lock that
has been active for more than the expected `duration`.

The catch: the cleaning function also requires a lock! It has been engineered 
in such way that:

-   The clear will try to clean it self! since it is the most important function
    and we don't want a forever locked entry
-   *The cleaner will clean it self only once and throw a ClusterCleanUpError*
    if it fails. This avoids recursive clean. If you get this exception **your
    context and the cleaner are locked - investigation needed**. The reasons 
    can be: (1) extreme load, (2) dead and locked cleaning process - killed while
    cleaning. If you see too many of those you need to tune the cleaning
    
#### Tuning the cleaner on a per-context basis

`cleanup_every` parameter: How often you want the cleaner to run. This changes depending
on the context. In general, this should be greater than `duration` To avoid 
overwhelming the cleaning function

## Internals

Two tables are created:

1.  `cluster_lock_ctx`: All known contexts and their lock status
1.  `cluster_lock_evt`: The currently active locks and their timestamps



    



    
    
