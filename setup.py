#!/usr/bin/env python


sdict = {
    'name': 'ppyqueue',
    'version': "0.2.1",
    'install_requires': ['ppyagent'],
    'packages': ['ppyqueue'],
    'package_dir': {'ppyqueue': 'ppyqueue'},
    'author': 'Li Chun',
    'author_email': 'lichun@papayamobile.com',
    'url': 'http://papayamobile.com',
    'classifiers': ['Environment :: Console',
                    'Intended Audience :: Developers',
                    'Programming Language :: Python']
}

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(**sdict)
