#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import asyncio
import asyncpg
from bs4 import BeautifulSoup

async def compress():
    reader = await asyncpg.connect(host='localhost', user='sunqf', password='840422', database='sunqf', command_timeout=60)
    writer = await asyncpg.connect(host='localhost', user='sunqf', password='840422', database='sunqf', command_timeout=60)

    with reader.transaction():
        async for record in reader.fetch('SELECT url, html, type from finished_url'):
            url = record['url']
            html = record['html']
            type = record['type']

            html = BeautifulSoup(html, 'html.parser')

