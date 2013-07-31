#Task Queue Worker

##Configs for Worker
The following configs are available for task queue worker.

+ __worker.port__ The port worker process listens on.
+ __worker.master_hosts__ A list of master process address for worker to connect.
+ __worker.ip_check_addr__ An address for get the address of server of the current worker process. 
+ __worker.queue_index__ The queue_index of current worker process. The worker will only do tasks in this queue.
+ __worker.max_sleep__ Max sleep time between two request to queue master if there is no task to do.
+ __worker.task_timeout__ When the task queue worker is in _dead_ state, i.e., the task queue worker process receives SIGINT/SIGUSR1/SIGUSR2 and will quit, if there is a running task, the worker process will wait up to __worker.task_timeout__ seconds. If the value is set to _None_, the worker process will keep waiting until the task finishes.

##Signal Control of Worker
The following signals may be use to control the state of worker process.

+ __SIGUSR1__ Gracefully stop the worker process, the worker will finish the running task and quit.
+ __SIGUSR2__ Gracefully stop the worker process, the worker will finish the running task and restart the worker process.
+ __SIGINT__ Gracefully stop the worker process, the worker will finish the running task and quit. If the PQWorkerAgent instance is initialized with _restart=True_, the worker will restart itself.

##Hooks for task execution
Several hooks may be injected in TaskWorker to do some AOP job over task execution. To inject a hook, use

    import ppyqueue.worker
    def hook_function(contect):
        pass
    hook_name = 'on_task_finished'
    ppyqueue.add_hook(hook_name, hook_function)
    
The _hook\_function_ has only one dict parameter context, which may contains different context key-value pair. The following hooks are supported:

+ __on\_task\_executing__
Called before a task is executed. Following may be contained in the _context_: 
 + _func_, the function object of the current task;
 + _sub_ and _kw_, the parameters from _func_;
 + _lang_ the language of the task pushed.
+ __on\_task\_finished__
Called after a task is finished(and no exception raised during execution process). Following may be contained in the _context_: 
 + _func_, the function object of the current task;
 + _sub_ and _kw_, the parameters from _func_;
 + _lang_ the language of the task pushed;
 + _return\_value_ the return value of the function;
+ __on\_task\_error__.
Called after a exception is raised during task executing process. Following may be contained in the _context_:
 + _exception_, the exception object.
