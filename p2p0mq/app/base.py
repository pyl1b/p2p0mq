# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging

logger = logging.getLogger('p2p0mq.app')


class BaseApp(object):
    """
    The base of our application.
    """
    def __init__(self, *args, **kwargs):
        """ Constructor. """
        super(BaseApp, self).__init__(*args, **kwargs)
