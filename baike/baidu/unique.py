#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import asyncio
from .. import db
from urllib.parse import unquote
from collections import Counter


async def fix():
    reader = await db.connect()
    writer = await db.connect()

    async with reader.transaction():
        async for record in reader.cursor('SELECT url, html, type FROM baike_html2'):
            url = unquote(record['url'])
            html = record['html']
            type = record['type']
            async with writer.transaction():
                await writer.execute('INSERT INTO baike_html (url, html, type) values ($1, $2, $3) ON CONFLICT DO NOTHING', url, html, type)


async def unique():
    reader = await db.connect()
    writer = await db.connect()

    prev_record = None
    async with reader.transaction():
        async for record in reader.cursor('SELECT url, html FROM baike_html ORDER BY url'):
            if prev_record and record['url'].startswith(prev_record['url']):
                if record['html'] == prev_record['html']:
                    async with writer.transaction():
                        await writer.execute('DELETE FROM baike_html WHERE url=$1', prev_record['url'])
                    prev_record = record
            else:
                prev_record = record

loop = asyncio.get_event_loop()
# loop.run_until_complete(fix())
loop.run_until_complete(unique())