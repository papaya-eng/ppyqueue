 # -*- coding: utf-8 -*-
'''
Papaya task queue worker.
'''
import logging
import time
import Pyro.core
import utility
import sys
from threading import Thread
import socket
import cStringIO
import cPickle
import ppyagent
import signal
import queue_config
logger = logging.getLogger('papaya.queue.worker')


def on_sigusr1(signum, frame):
    if signum == signal.SIGUSR1:
        print "Receive SIGUSR1."
        PQWorkerAgent._restart = False
        raise KeyboardInterrupt("Stop server gracefully.")


def on_sigusr2(signum, frame):
    if signum == signal.SIGUSR2:
        print "Receive SIGUSR2."
        PQWorkerAgent._restart = True
        raise KeyboardInterrupt("Stop server gracefully and restart itself.")

signal.signal(signal.SIGUSR1, on_sigusr1)
signal.signal(signal.SIGUSR2, on_sigusr2)


class TaskDoer:
    '''
    A wrapper for executing a task.
    '''
    stats = {}
    hooks = {}

    def __init__(self, func, fclass=None, module=None,
                 path=None, parameters=None, lang=None):
        '''
        :param func: The name of the function.
        :param fclass: The name of the class of the function,
               None means the function is not a class method.
        :param module: The name of module of the function,
               None means already imported in globals().
        :param path: The path of the module.
        :param parameters: (sub, kw) tuple of parameters.
        '''
        self._func = func
        self._fclass = fclass
        self._module = module
        self._path = path
        self._parameters = parameters
        self._lang = lang
        self._unpickler = cPickle.Unpickler

    def _get_func(self):
        module = fclass = None
        if self._path and (not self._path in sys.path):
            sys.path.append(self._path)
        if self._module:
            if not self._module in sys.modules:
                __import__(self._module)
            module = sys.modules[self._module]
        if self._fclass:
            fclass = globals()[self._fclass]\
                if not self._module else getattr(module, self._fclass)
        if self._fclass:
            func = getattr(fclass, self._func)
        else:
            func = globals()[self._func]\
                if not self._module else getattr(module, self._func)
        return func

    def _get_parameters(self):
        if self._parameters is not None:
            qu = self._unpickler(cStringIO.StringIO(self._parameters))
            r = qu.load()
        else:
            r = []
        sub = r[0] if len(r) >= 1 else []
        kw = r[1] if len(r) >= 2 else {}
        return (sub, kw)

    def _run_hook(self, hook_name, **params):
        hook = self.hooks.get(hook_name, None)
        if hook:
            try:
                context = {}
                context.update(params)
                hook(context)
            except Exception, e:
                logger(e)

    def __call__(self):
        ret = {}
        try:
            start_time = time.time()
            func = self._get_func()

            sub, kw = self._get_parameters()
            self._run_hook('on_task_executing', func=func, sub=sub, kw=kw,
                           lang=self._lang)
            return_value = func(*sub, **kw)
            logger.info(str(return_value))
            self._run_hook('on_task_finished', func=func, sub=sub, kw=kw,
                           lang=self._lang, return_value=return_value)
            ret['status'] = 1
        except Exception, e:
            if type(e) is utility.RetryException:
                logger.error(str(e))
            else:
                logger.exception(e)
            self._run_hook('on_task_error', exception=e)
            ret['status'] = 0
        finally:
            end_time = time.time()
            func_key = (self._func, self._fclass, self._module)
            if not TaskDoer.stats.get(func_key):
                TaskDoer.stats[func_key] = [0.0, 0, 0]
            TaskDoer.stats[func_key][0] += end_time - start_time
            TaskDoer.stats[func_key][1] += 1
            TaskDoer.stats[func_key][2] += ret.get('status', 0)

        return ret


def add_hook(hook_name, func):
    TaskDoer.hooks[hook_name] = func


class PQWorker(Thread):
    def __init__(self, **kw):

        settings = queue_config.SETTINGS
        self._server = settings.get('worker.master_hosts', [])
        self._my_ip_addr = self._get_my_ip_addr(
            *settings.get('worker.ip_check_addr'))
        self._my_port = settings.get('worker.port')
        self._queue_index = settings.get('worker.queue_index')
        self._max_sleep = settings.get('worker.max_sleep')

        self._running_info = {}

        self._pyro_call = ppyagent.PyroFunc(self._server, retry=1,
                                            random_idx=True, threaded=True)
        self._dead = False
        self._sleep = False
        Thread.__init__(self)

    def _get_my_ip_addr(self, host, port):
        my_ip_addr = '127.0.0.1'
        for i in range(2):
            try:
                sock = socket.socket()
                sock.connect((host, port))
                my_ip_addr = sock.getsockname()[0]
                sock.close()
                return my_ip_addr
            except:
                pass

    def get_worker_info(self):
        info = {'host': self._my_ip_addr, 'port': self._my_port}
        return info

    def get_running_info(self):
        return self._running_info

    def run(self):
        '''
        The main loop of a worker process.
        '''
        sleep_time = 0
        total_sleep_time = 0.0
        piggyback_task = None
        while True:
            if self._dead and not piggyback_task:
                print "Quit worker loop"
                break
            if piggyback_task is None:
                r = self._pyro_call('get_task',
                                    self.get_worker_info(),
                                    queue_index=self._queue_index)
            else:
                r = piggyback_task
                logger.info("Got a piggyback task.")
                piggyback_task = None

            if r is None:
                sleep_time = (sleep_time + 0.01) * 2
                sleep_time = sleep_time if sleep_time < self._max_sleep\
                    else self._max_sleep
            elif r == 'invalid':
                pass
            else:
                sleep_time = 0
                total_sleep_time = 0
                self._running_info = {'id': r['id']}
                td = TaskDoer(r['func'], fclass=r['fclass'],
                              module=r['module'],
                              path=r['path'], lang=r.get('lang'),
                              parameters=r['parameters'])
                logger.info("Doing Task ID:%s, %s on %s:%s." % (r['id'],
                            r['func'], self._my_ip_addr, self._my_port))
                result = td()

                piggyback = False if self._dead else True
                if result['status'] == 1:
                    ret = self._pyro_call('task_finish', r['id'],
                                          self.get_worker_info(),
                                          queue_index=self._queue_index,
                                          piggyback=piggyback)
                else:
                    ret = self._pyro_call('task_error', r['id'],
                                          self.get_worker_info(),
                                          queue_index=self._queue_index,
                                          piggyback=piggyback)
                self._running_info = {}
                __, piggyback_task = ret
            if sleep_time > 0:
                sys.stdout.write("\r" + " " * 80)
                sys.stdout.write("\rSleeping for over %s seconds..."
                                 % total_sleep_time)
                sys.stdout.flush()
            self._sleep = True
            time.sleep(sleep_time)
            total_sleep_time += sleep_time
            self._sleep = False


class PQWorkerAgent(Pyro.core.ObjBase):
    def __init__(self, restart=False):
        Pyro.core.ObjBase.__init__(self)
        self._worker = PQWorker()
        PQWorkerAgent._restart = restart

    def get_running_info(self):
        r = {}
        r.update(self._worker.get_running_info())
        r['sleep'] = self._worker._sleep
        r['dead'] = self._worker._dead
        return r

    def get_worker_stats(self):
        stats = {}
        for key, value in TaskDoer.stats.items():
            d = {'total_time': value[0],
                 'total_access': value[1],
                 'succ_access': value[2],
                 'avg_time': value[0] / value[1]
                 }
            stats[key] = d
        return stats

    def on_close(self):
        print "Shutdown queue worker."
        self._worker._dead = True
        if self._worker._sleep is True:
            utility.keyboard_interrupt()
            utility.restart_self(0, self._restart)
        task_timeout = queue_config.SETTINGS.get('worker.task_timeout', 0)
        self._worker.join(task_timeout)
        utility.keyboard_interrupt()
        utility.restart_self(0, PQWorkerAgent._restart)

    def start_server(self):
        Pyro.core.initServer()
        self._worker.start()
        daemon = Pyro.core.Daemon(host='0.0.0.0',
                                  port=self._worker._my_port)
        daemon.connect(self, "worker")
        logger.info("Queue worker start at %s:%s for queue %s."
                    % (self._worker._my_ip_addr,
                       self._worker._my_port, self._worker._queue_index))
        try:
            daemon.requestLoop()
        except KeyboardInterrupt, e:
            Thread(target=self.on_close).start()
            try:
                daemon.requestLoop()
            except KeyboardInterrupt, e:
                daemon.shutdown()
                print "The daemon is shutdown"
