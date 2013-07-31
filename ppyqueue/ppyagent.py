# -*- coding: utf-8 -*-
import Pyro.core
import logging
import time
import threading
import random

logger = logging.getLogger("ppyagent")


def PyroFunc(*sub, **kw):
    '''
    A unified interface of a pyro function call.
    This a factory method for generating desired PyroFuncObj class,
    if threaded is True in parameters, a sub class of threading.local
    instead of object will be generated .
    '''
    if not kw.get('threaded'):
        cls = type('PyroFuncObj', (object,), {})
    else:
        cls = type('PyroFuncObj', (threading.local, ), {})

    def __init__(self, uri='', func='', default_return=None, retry=1,
                 random_idx=False, threaded=False, set_one_way=False,
                 **kw):
        '''
        :param uri: the address of a pyro server,
               or a list of addresses.
        :param func: the name of the func.
        :param default_return: default return value if exception occured.
        :param retry: max retry times if previous call failed.
        :param random_idx: randomly choose one address from uri when init.
        :param threaded: Maintain a client for each thread if True,
               create a client each call if False.
        :param set_one_way: Do not wait for return value of pyro call.
        '''
        self._uri = (uri, ) if type(uri) == str else uri
        self._current_uri_idx = 0 if not random_idx else\
            random.randint(0, len(self._uri) - 1)
        self._func = func
        self._default_return = default_return
        self._retry = retry
        self._threaded = threaded
        self._set_one_way = set_one_way
        self._client = None

    def _uri_idx_next(self, curr):
        '''
        Get next uri in the list randomly.
        '''
        if self._current_uri_idx == curr:
            incr = random.randint(1, len(self._uri) - 1 if
                                  len(self._uri) > 2 else 1)
            self._current_uri_idx = (curr + incr) % len(self._uri)

    def __call__(self, *sub, **kw):
        '''
        Interface for calling the pyro function.
        '''
        if self._func:
            return self._do(self._func, *sub, **kw)
        else:
            return self._do(sub[0], *sub[1:], **kw)

    def _do(self, _func, *sub, **kw):
        '''
        The actual code for calling pyro function.
        '''
        for i in range(self._retry + 1):
            idx = self._current_uri_idx
            try:
                if not self._client:
                    self._client = Pyro.core.getProxyForURI(self._uri[idx])
                    if self._set_one_way:
                        self._client._setOneway((_func,))
                return getattr(self._client, _func)(*sub, **kw)
            except Exception, e:
                self._client = None
                logger.exception(e)
                self._uri_idx_next(idx)
                time.sleep(0.1)
            finally:
                if not self._threaded:
                    self._client = None
        return self._default_return

    cls.__init__ = __init__
    cls._uri_idx_next = _uri_idx_next
    cls.__call__ = __call__
    cls._do = _do

    return cls(*sub, **kw)
