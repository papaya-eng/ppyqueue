# -*- coding: utf-8 -*-
'''
Papaya task queue master.
'''
from __future__ import with_statement
try:
    import gevent.monkey
    gevent.monkey.patch_all()
except Exception, e:
    print("gevent patching failed.")
    raise e
from redis import Redis
import logging
import time
import cPickle
import random
import datetime
import threading
import queue_config
import ppyagent
import Pyro.core
import os
import utility

utility.claim_limit()

logger = logging.getLogger('papaya.queue.master')


class redis_client(object):
    def __init__(self, queue_redis_map):
        '''
        :param queue_redis_map:
        A dict map queue index number to
        (host, port, db_number) tuple of
        a redis server.
        '''
        self.clients = [None] * len(queue_redis_map)
        for i in queue_redis_map:
            host, port, db_number = queue_redis_map[i]
            self.clients[i] = Redis(host=host, port=port, db=db_number)
            self.clients[i].connection_pool.max_connections = 64

    def __getitem__(self, i):
        return self.get(i)

    def get(self, queue_index=0):
        return self.clients[queue_index]


class redislock(object):
    '''
    A Lock for redis.
    '''
    def __init__(self, name, client, expires=60):
        '''
        :param name: The name of lock.
        :param client: The redis client instance.
        :param expires: The expire time in seconds for the lock.
        '''
        self.client = client
        self.name = 'l%s' % name
        self.expires = 60

    def __enter__(self):
        while True:
            expires = time.time() + self.expires
            if self.client.setnx(self.name, expires):
                # Get the lock, set the expire in case lock never released
                self.client.expire(self.name, self.expires)
                break
            current_value = self.client.get(self.name)

            # We found an expired lock and nobody raced us to replacing it
            if current_value and float(current_value) < time.time() and \
                    self.client.getset(self.name, expires) == current_value:
                self.client.expire(self.name, self.expires)
                break
            time.sleep(0.01)
        return

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.delete(self.name)


class redislock2(object):
    '''
    A redis distributed lock. Use redis.set with ex and nx
    parameters enabled. The lock should be used only with
    redis-py >= 2.7.4 and redis >= 2.6.12.

    The redislock2 has less redis communication than redislock.
    '''
    def __init__(self, name, client, expires=60):
        '''
        parameters
        ----------
        name: strnig
            The name of the lock.
        client:
            The redis-py client instance.
        expires: int, default 60
            The lock will be expired after 60 seconds.
        '''
        self.client = client
        self.name = 'l%s' % name
        self.expires = 60

    def __enter__(self):
        while not self.client.set(self.name, '1', ex=self.expires, nx=True):
            time.sleep(0.01)

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.delete(self.name)

# Uncomment this line if redis-py >= 2.7.4 and redis >= 2.6.12
# redislock = redislock2
# You may also write
# ppyqueue.master.redislock = ppyqueue.master.redislock2
# to overwrite redislock


class BaseStore:
    ATTRS = [('key', ''), ('state', 0), ('func', ''), ('fclass', ''),
             ('module', ''), ('path', ''), ('lang', ''), ('parameters', ''),
             ('retry', 0), ('max_retry', 1), ('retry_delay', False),
             ('time', None), ('worker_info', {}), ('delay', None)]

    @classmethod
    def get_attrs(cls, d):
        return dict([(k[0], d.get(k[0], k[1])) for k in cls.ATTRS])

    def clear(self):
        pass


class RedisStore(BaseStore):
    @classmethod
    def pack(cls, data):
        return cPickle.dumps(data, protocol=2)

    @classmethod
    def unpack(cls, data):
        return cPickle.loads(data)

    def getlock(self, name):
        return redislock(name, self._store)

    def __init__(self, store):
        self._store = store
        self._empty_queue = False

    def assign_id(self):
        return ''.join(random.choice('0123456789abcdef') for i in xrange(32))
        #uuid is not thread safe!!!
        #return uuid.uuid4().hex

    def enqueue_task(self, task_id, pipe=None):
        if pipe is None:
            self._store.rpush('queued_tasks', task_id)
        else:
            pipe.rpush('queued_tasks', task_id)
        self._empty_queue = False

    def new_task(self, attributes):
        '''
        Store attributes
        '''
        task_id = self.assign_id()
        attributes = self.__class__.get_attrs(attributes)
        attributes['state'] = 0
        attributes['time'] = datetime.datetime.now()
        pipe = self._store.pipeline(transaction=False)
        pipe.set('t%s' % task_id, self.__class__.pack(attributes))
        if attributes['key']:  # check consistent
            pipe.set('k%s' % attributes['key'], task_id)
        delay = attributes.pop('delay', None)
        if delay:
            pipe.execute()
            self.delay_task(task_id, attributes, delay)
        else:
            self.enqueue_task(task_id, pipe=pipe)
            pipe.execute()
        #self._store.set('t%s' % task_id, self.__class__.pack(attributes))
        #if attributes['key']:  # check consistent
        #    self._store.set('k%s' % attributes['key'], task_id)
        #delay = attributes.pop('delay', None)
        #if delay:
        #    self.delay_task(task_id, attributes, delay)
        #else:
        #    self.enqueue_task(task_id)
        return task_id

    def get_task_info(self, task_id):
        '''
        Get stored task info
        '''
        task = self._store.get('t%s' % task_id)
        if task:
            task = cPickle.loads(task)
        return task

    def get_first_undone_task(self, worker_info):
        '''
        Return first undone task as an dict
        '''
        #if self._empty_queue: return None
        task_id = self._store.lpop('queued_tasks')
        if task_id is None:
            #self._empty_queue = True
            return None
        with self.getlock(task_id):
            task = self._get_task(task_id)
            if task is None:
                return None
            task['worker_info'] = worker_info
            task['state'] = 1
            pipe = self._store.pipeline(transaction=False)
            pipe.set('t%s' % task_id, self.__class__.pack(task))
            pipe.sadd('running_tasks', task_id)
            pipe.execute()
            return (task_id, task)

    def get_store_info(self):
        if not self._store.exists('queued_tasks'):
            return None
        ret = {'delayed_tasks_size': self._store.zcard('delayed_tasks'),
               'queued_tasks_size': self._store.llen('queued_tasks'),
               'running_tasks': str(self._store.smembers('running_tasks'))}
        return ret

    def _get_task(self, id):
        task = self._store.get('t%s' % id)
        if task is not None:
            task = self.__class__.unpack(task)
        return task

    def get_task(self, id):
        task = self._get_task(id)
        return [id, task] if task else None

    def get_task_by_key(self, key):
        task_id = self._store.get('k%s' % key)
        if task_id is None:
            return None
        task = self._get_task(task_id)
        return (task_id, task) if task else None

    def get_running_tasks(self):
        task_ids = self._store.smembers('running_tasks')
        for task_id in task_ids:
            task = self._get_task(task_id)
            if task:
                yield task_id, task
            else:
                self._store.srem('running_tasks', task_id)

    def check_worker_info(self, id, worker_info={}):
        task = self._get_task(id)
        if task and (task['worker_info'].get('host'),
                     task['worker_info'].get('port')) == \
                (worker_info.get('host'), worker_info.get('port')):
            return True
        else:
            return False

    def destroy_task(self, id):
        with self.getlock(id):
            pipe = self._store.pipeline(transaction=False)
            pipe.delete('t%s' % id)
            pipe.srem('running_tasks', id)
            pipe.execute()
            # TODO: get task content check the key attr,
            # use a more elaborated way to handle it

    def change_task(self, id, attributes=None, force=False):
        '''
        Change the attributes of a task. If force is set to True,
        the task attributes will be changed even the task is in
        running state.
        '''
        if attributes is None:
            attributes = {}
        with self.getlock(id):
            task = self._get_task(id)
            if task is None:
                return False  # empty task

            # Do not change running task unless forced
            if (not force) and task.get('state') == 1:
                return False

            # Remove from running_task if the state of the
            # changed from 1 to 0.
            if task.get('state') == 1 and attributes.get('state') == 0:
                self._store.srem('running_tasks', id)

            task.update(attributes)
            self._store.set('t%s' % id, self.__class__.pack(task))
            if attributes['key']:  # check consistet
                self._store.set('k%s' % attributes['key'], id)
            self._empty_queue = False
            return True

    def first_delay(self):
        '''
        Get the timestamp of the first delayed task in current storage.
        '''
        if not self._store.exists('delayed_tasks'):
            return None
        r = self._store.zrange('delayed_tasks', 0, 0, withscores=True)
        if r:
            return r[0][1]
        else:
            return None

    def delay_task(self, id, attrs, delay=1):
        '''
        Put a task in delayed queue.
        parameters
        ----------
        id: The task id.
        attrs: dict,
            The attributes of task will be updated with attrs.
        delay: int,
            The seconds for the task to be delayed.
        '''
        t = time.time() + delay
        self._store.srem('running_tasks', id)
        if self.change_task(id, attributes=attrs, force=True):
            self._store.zadd('delayed_tasks', id, t)
            return t
        else:
            return None

    def push_delayed_task(self):
        '''
        Pop the ids of delayed tasks which are to be enqueued.
        '''
        ids = []
        with self.getlock('delayed_lock'):
            r = self._store.zrangebyscore('delayed_tasks', 0, time.time(),
                                          withscores=True)
            for id, delay in r:
                ids.append(id)
                self._store.zrem('delayed_tasks', id)
        return ids


class PQServer(Pyro.core.ObjBase):
    def __init__(self, **kw):
        Pyro.core.ObjBase.__init__(self)
        self._queue_redis_map = queue_config.SETTINGS.get('master.redis_map')
        self._max_queue_number = len(self._queue_redis_map)
        db_clients = redis_client(self._queue_redis_map)
        self._stores = {}
        for i in self._queue_redis_map:
            self._stores[i] = RedisStore(store=db_clients[i])

    def start_server(self, port=7600):
        try:
            port = queue_config.SETTINGS.get('master.port') or port
            threading.Thread(target=self.check_agent).start()
            threading.Thread(target=self.check_delayed_tasks).start()
            Pyro.config.PYRO_MAXCONNECTIONS = 10000
            Pyro.core.initServer()
            #if not is_port_free(port):
            #    print "Can not bind to port %s."%port
            #    sys.exit(1)
            logger.info("Queue Server start at 0.0.0.0:%s." % port)
            daemon = Pyro.core.Daemon(host='0.0.0.0', port=port)
            daemon.connect(self, "queue")
            daemon.requestLoop()
        except KeyboardInterrupt, e:
            print "Shutdown queue server."
            daemon.shutdown()
            os._exit(0)

    def check_delayed_tasks(self):
        self._delay = {}
        for i in self._stores:
            self._delay[i] = self._stores[i].first_delay()
        while True:
            t = time.time()
            for i, delay in self._delay.items():
                if delay and t > delay:
                    self._delayed_tasks(i)
                    self._delay[i] = self._stores[i].first_delay()
                elif delay is None:
                    if random.random() < 0.1:
                        self._delay[i] = self._stores[i].first_delay()
            time.sleep(2)

    def push_task(self, func, key='', fclass=None, module=None, path=None,
                  lang='', parameters=None, max_retry=1, retry_delay=False,
                  queue_index=0, delay=None):
        attributes = dict(func=func, key=key, fclass=fclass, module=module,
                          path=path, lang=lang, parameters=parameters,
                          max_retry=max_retry, retry_delay=retry_delay,
                          delay=delay)
        attributes = BaseStore.get_attrs(attributes)
        changed = False
        id = None
        store = self._stores[queue_index]
        if key:
            task = store.get_task_by_key(key)
            if task and store.change_task(task[0], attributes):
                changed = True
                id = task[0]
        if not changed:
            id = store.new_task(attributes)
        store.clear()
        logger.info("PUSH_TASK %s %s in queue %s" % (id, func, queue_index))
        return id

    def get_task_info(self, task_id, queue_index=0):
        store = self._stores[queue_index]
        return store.get_task_info(task_id)

    def get_task(self, worker_info, queue_index=0):
        '''
        get_task(self, worker_infO)
        Get a task from queue master.
        '''
        if type(worker_info) != dict:
            worker_info = {}
        store = self._stores[queue_index]
        task = store.get_first_undone_task(worker_info)
        store.clear()
        if task is None:
            return None
        elif task == 'invalid':
            return 'invalid'
        else:
            ret = {'id': task[0], 'func': task[1]['func'],
                   'fclass': task[1]['fclass'], 'module': task[1]['module'],
                   'path': task[1]['path'], 'lang': task[1].get('lang'),
                   'parameters': task[1]['parameters']}
            logger.info("GET_TAsk %s %s %s:%s in queue %s" % (task[0],
                        task[1]['func'], worker_info.get('host'),
                        worker_info.get('port'), queue_index))
            return ret

    def task_finish(self, id, worker_info, queue_index=0, piggyback=False):
        store = self._stores[queue_index]
        task = store.get_task(id)
        ret = None
        if task and store.check_worker_info(id, worker_info):
            store.destroy_task(id)
            ret = True
            logger.info("TASK_FINISH %s %s %s:%s in queue %s" % (id,
                        task[1]['func'], worker_info.get('host'),
                        worker_info.get('port'), queue_index))
        else:
            ret = False

        piggyback_task = None if not piggyback else\
                         self.get_task(worker_info, queue_index=queue_index)
        store.clear()
        return ret, piggyback_task

    def task_error(self, id, worker_info, queue_index=0, piggyback=False):
        ret = None
        store = self._stores[queue_index]
        task = store.get_task(id)
        if task and store.check_worker_info(id, worker_info):
            task[1]['worker_info'] = {}
            ret = True
            if task[1]['retry'] >= task[1]['max_retry']:
                store.destroy_task(id)
                logger.info("TASK_ERROR %s %s %s:%s in queue %s" % (id,
                            task[1]['func'], worker_info.get('host'),
                            worker_info.get('port'), queue_index))
            elif not task[1].get('retry_delay'):
                logger.info("TASK_ERROR %s %s %s:%s in queue %s" % (id,
                            task[1]['func'], worker_info.get('host'),
                            worker_info.get('port'), queue_index))
                #store.destroy_task(id)
                task[1]['retry'] += 1
                task[1]['worker_info'] = {}
                task[1]['state'] = 0
                store.change_task(id, task[1], force=True)
                #store.new_task(task[1])
                store.enqueue_task(id)
            else:
                task[1]['retry'] += 1
                task[1]['state'] = 0
                delay = 1 + 2 ** task[1]['retry']
                self._delay_task(id, task[1], delay, queue_index)
                logger.info("TASK_ERROR %s %s %s:%s in queue %s, delayed %s "
                            % (id, task[1]['func'], worker_info.get('host'),
                            worker_info.get('port'), queue_index, delay))
        else:
            ret = False

        piggyback_task = None if not piggyback else\
                         self.get_task(worker_info, queue_index=queue_index)
        store.clear()
        return ret, piggyback_task

    def check_agent(self):
        '''
        Check all queue stores for failed tasks.
        '''
        while 1:
            time.sleep(random.randint(100, 900))
            try:
                i = random.choice(self._stores.keys())
                self.check_running_task(i)
            except Exception, e:
                logger.exception(e)
            time.sleep(random.randint(0, 7200))

    def check_running_task(self, queue_index=0):
        logger.info("Checking the running tasks of queue %s." % queue_index)
        store = self._stores[queue_index]
        for id, task in store.get_running_tasks():
            if not task['worker_info']:
                if task['state'] == 1:
                    task['worker_info'] = {}
                    task['state'] = 0
                    if store.change_task(id, task, force=True):
                        store.enqueue_task(id)
                continue
            pc = ppyagent.PyroFunc('PYROLOC://%s:%s/worker'
                                % (task['worker_info']['host'],
                                task['worker_info']['port']),
                                default_return={})
            info = pc('get_running_info')
            if info.get('id') != id:
                if task['state'] == 1:
                    task['worker_info'] = {}
                    task['state'] = 0
                    if store.change_task(id, task, force=True):
                        store.enqueue_task(id)
        store.clear()

    def _delay_task(self, id, task_attrs, delay, queue_index=0):
        '''
        Make a existed task to be delayed.
        '''
        store = self._stores[queue_index]
        delayed_time = store.delay_task(id, task_attrs, delay=delay)
        if delayed_time is None:
            return
        if self._delay[queue_index] is None or\
                delayed_time < self._delay[queue_index]:
            self._delay[queue_index] = delayed_time

    def _delayed_tasks(self, queue_index=0):
        '''
        Check delayed task, and push those ready to run.
        '''
        store = self._stores[queue_index]
        delayed_ids = store.push_delayed_task()
        for id in delayed_ids:
            task = store.get_task(id)
            if task:
                #store.destroy_task(id)
                task[1]['worker_info'] = {}
                task[1]['state'] = 0
                if store.change_task(id, task[1], force=True):
                    store.enqueue_task(id)
                #store.new_task(task[1])

    def server_info(self):
        ret = []
        for index, store in self._stores.items():
            info = store.get_store_info()
            if info:
                info['queue_index'] = index
                ret.append(info)
        return ret
