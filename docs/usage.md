#Basic Usage of ppyqueue

_ppyqueue_ provides core function for establishing the task queue structures. Basically, the task queue composes of Redis servers, queue master processes and queue worker processes.

##Deploy queue masters
Assuming you have a redis server at _127.0.0.1:6379_, you may setup a queue master server by:

    #test_master.py
    import ppyqueue.master as master
    import ppyqueue.queue_config as queue_config
    import logging
    
    # setup logger
    logger = logging.getLogger('papaya.queue.master')
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    # modify setting
    # master.redis_map means you map db 0 of redis
    # server 127.0.0.1:6379 to queue_index 0,
    # you may also add other db or redis server for
    # different queue_index.
    settings = {'master.redis_map': {0: ('127.0.0.1', 6379, 0)},
                'master.port': 7600}
    queue_config.SETTINGS.update(settings)

    # start the queue
    pq_server = master.PQServer()
    pq_server.start_server()

Note the concept of _queue\_index_ is the index number of a distributed queue, all tasks pushed into this queue will be queued in the same queue storage and therefore will be poped orderly.  Start _test\_master.py_, we shall have a task queue master listen on port 7600.

##Deploy queue workers
Now we can make queue worker server connecting to the queue master:

    #test_worker.py
    import ppyqueue.worker as worker
    import ppyqueue.queue_config as queue_config
    import logging

    # setup logger
    logger = logging.getLogger('papaya.queue.worker')
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    # modify setting
    settings = {'worker.port': 7620,
                'worker.master_hosts': ['PYROLOC://127.0.0.1:7600/queue'], 
                'worker.queue_index': 0,
                'worker.max_sleep': 12}

    queue_config.SETTINGS.update(settings)

    # start the worker
    worker = worker.PQWorkerAgent()
    worker.start_server()

Start _test\_worker.py_, we shall have our worker connecting to the queue master, and itself will listen on 7620.

##Push tasks to the queue
You need to use _ppyqueue.ppyagent_ for pushing a task to target server.

    import ppyqueue.ppyagent as ppyagent
    import cPickle

    client = ppyagent.PyroFunc('PYROLOC://127.0.0.1:7600/queue', retry=1,
                               random_idx=True, threaded=True)

    def push(func, **d):
        sub = d.pop('sub', [])
        kw = d.pop('kw', {})
        d['parameters'] = cPickle.dumps( (sub,kw) )

        client('push_task', func, **d)
       
    #push a task
    push('time', module='time', queue_index=0)    

Note that you should make sure the module can be imported in queue workers directory.
