#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

## Broker settings.
broker_url = 'pyamqp://sunqf:840422@localhost/baike'

# List of modules to import when the Celery worker starts.
imports = ('baike.tasks',)

## Using the database to store task state and results.
result_backend = 'rpc://'
result_persistent = False

# task_annotations = {'tasks.add': {'rate_limit': '10/s'}}

task_compression = 'zlib'

