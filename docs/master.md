#Task Queue Master

##Configs for Master
+ __master.port__ The port master procsss listens on.
+ __master.redis_map__ A dict mapping the queue_index to its redis storage. The key of the dict is queue_index, the value is a 3-tuple of (REDIS_SERVER_HOST, REDIS_SERVER_PORT, REDIS_SERVER_DB_NUM). Below is an example of the _redis\_map_:

    redis_map = {0: ('127.0.0.1', 6379, 0),
                 1: ('127.0.0.1', 6379, 1),
                 2: ('192.168.1.2', 6379, 0)}  # map to another server

##Notes about implementation
###gevent
The queue master process will use gevent monkey patch to increase the parallelism. If no gevent is provided, the master process is not going to start. Yet in one process, should not import worker before master.

###ulimit claim
The queue master process will call _utility.claim\_limit()_ to setup the ulimit for current process. If the current user is not root, the function will fail and be ignored.

###Collectting fail tasks
A thread in queue master will check all the running tasks of a randomly selected queue, see if the queue worker are still reachable or whether the task on the queue is correct. The thread will sleep for random seconds between every two checks.

###Delayed jobs
A thread in queue master will check the priority queue in redis, see if there are delayed tasks could be pushed back into the queue.

###Piggyback
The _task\_error_ and _task\_finish_ functions will deal the case when a task is done on the worker. If a _piggyback_ parameter is pass as _True_, the two functions will also call the _get\_task_ functions to try fetch a new task and return it.
