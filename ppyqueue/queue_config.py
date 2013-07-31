'''
Config module of Papaya Queue.
'''

SETTINGS = {'master.port': 7600,
            'master.redis_map': {},

            'worker.port': 7620,
            'worker.master_hosts': [],
            'worker.queue_index': None,
            'worker.ip_check_addr': ['127.0.0.1', 22],
            'worker.max_sleep': 10,
            'worker.task_timeout': None}
