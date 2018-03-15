#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import asyncpg
def connect():
    return asyncpg.connect(host='localhost', user='sunqf', password='840422', database='sunqf', command_timeout=60)