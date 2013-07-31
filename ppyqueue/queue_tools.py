# -*- coding: utf-8 -*-
'''
Tools for Papaya task queue.
'''
import pickle
import sys


class QueueUnpickler(pickle.Unpickler):
    get_session = None
    def __init__(self, *sub, **kw):
        pickle.Unpickler.__init__(self, *sub, **kw)
        self.entity_set = set()

    def load(self, *sub, **kw):
        r = pickle.Unpickler.load(self, *sub, **kw)
        if self.get_session is not None:
            session = self.get_session
            for inst in self.entity_set:
                session.merge(inst)
        return r

    def find_class(self, module, name):
        if 'model' in module:
            return globals()[name]
        else:
            if module not in sys.modules:
                __import__(module)
            mod = sys.modules[module]
            klass = getattr(mod, name)
            return klass

    def load_build(self):
        '''
        Add sqlalchemy instances to the running sessions
        '''
        pickle.Unpickler.load_build(self)
        stack = self.stack
        inst = stack[-1]
        if type(inst).__class__.__name__ == 'EntityMeta':
            self.entity_set.add(inst)
QueueUnpickler.dispatch[pickle.BUILD] = QueueUnpickler.load_build
