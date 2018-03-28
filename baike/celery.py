#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from celery import Celery
from baike import celeryconfig

'''
app = Celery('tasks',
             backend='rpc://',
             broker='pyamqp://sunqf:840422@localhost/baike',
             include=['baike.tasks'])
'''

app = Celery()
app.config_from_object(celeryconfig)