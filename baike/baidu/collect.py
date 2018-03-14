#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-


import asyncio
import asyncpg
from collections import Counter
from pprint import pprint
import json


async def collect():
    reader = await asyncpg.connect(host='localhost', user='sunqf', password='840422',
                                   database='sunqf',
                                   command_timeout=60)
    counter = Counter()
    async with reader.transaction():
        async for r in reader.cursor("select knowledge -> 'attrs' -> 'open_tags' as tags from baike_knowledge2"):
            tags = r['tags']
            if tags:
                counter.update(json.loads(tags))

    return counter

loop = asyncio.get_event_loop()
counter = loop.run_until_complete(collect())
pprint(counter)
