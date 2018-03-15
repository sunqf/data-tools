#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from typing import List
import asyncpg
import json
import asyncio
from .. import db
from bs4 import BeautifulSoup
aliases = ['简称', '另称', '人称', '别称', '别名', '别号', '字号', '笔名', '又称']

async def extract():
    reader = await db.connect()
    writer = await db.connect()

    query = 'select url, knowledge->\'title\' as title, knowledge->\'attrs\'->\'infobox\' as infobox' \
            ' from baike_knowledge2 where (knowledge->\'attrs\'->\'infobox\')::jsonb ?| array[{}]'\
        .format(','.join(['\'' + a + '\'' for a in aliases]))

    print(query)
    async with reader.transaction():
        async for record in reader.cursor(query):
            url = record['url'].strip()
            title = record['title']
            infobox = record['infobox']
            if infobox:
                infobox = json.loads(infobox)
                mentions = set()
                for alias in aliases:
                    if alias in infobox:
                        for l in infobox[alias]:
                            div = BeautifulSoup(l, 'html.parser')
                            text = div.text
                            if text.startswith('字：'):
                                text = text[2:]
                            elif text.startswith('字'):
                                text = text[1:]

                            mentions.add((text, title, url))

                async with writer.transaction():
                    await writer.executemany('INSERT INTO mention2entity(mention, title, url) VALUES($1, $2, $3) ON CONFLICT DO NOTHING',
                                       mentions)

loop = asyncio.get_event_loop()
loop.run_until_complete(extract())

