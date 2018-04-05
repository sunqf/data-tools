#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import asyncpg


def connect():
    return asyncpg.connect(host='localhost',
                           user='sunqf',
                           database='bilingual',
                           command_timeout=60)

def connect_pool():
    return asyncpg.create_pool(host='localhost', user='sunqf', database='bilingual')