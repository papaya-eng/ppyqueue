# -*- coding: utf-8 -*-
'''
Utility library for Papaya task queue.
'''
import resource
import sys
import os
import subprocess


def claim_limit(limit=50000):
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, limit))
    except Exception, e:
        pass


class RetryException(Exception):
    pass


def fd_ensure():
    pid = os.getpid()
    link_stdout = os.readlink('/proc/%s/fd/1' % pid)
    link_stderr = os.readlink('/proc/%s/fd/2' % pid)
    if link_stdout.endswith('(deleted)') or link_stderr.endswith('(deleted)'):
        s = link_stdout[:-10]
        f = open(s, 'w')
        os.chmod(s, 0666)
        os.dup2(f.fileno(), sys.stdout.fileno())
        os.dup2(f.fileno(), sys.stderr.fileno())


def restart_self(code, restart=False):
    if restart:
        fd_ensure()
        pid = os.getpid()
        cmd_line = open('/proc/%s/cmdline' % pid).read()
        cmd_line = cmd_line.replace('\x00', ' ').strip()
        p = subprocess.Popen(cmd_line,  shell=True, close_fds=True)
        if p.pid:
            os.system('kill %s' % p.pid)  # kill /bin/sh process
        sys.stdout.truncate(0)
        sys.stderr.truncate(0)
    os._exit(code)


def keyboard_interrupt():
    os.kill(os.getpid(), 2)
