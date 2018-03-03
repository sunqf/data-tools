#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import asyncio
import asyncpg
import pathlib
import json


async def extract():

    pool = await asyncpg.create_pool(host='localhost', user='sunqf', database='sunqf', command_timeout=60)

    async with pool.acquire() as reader:
        async with pool.acquire() as writer:

            async with reader.transaction():
                # Postgres requires non-scrollable cursors to be created
                # and used in a transaction.
                async for record in reader.cursor('SELECT category, data from dictall'):
                    category = record['category']
                    pairs = json.loads(record['data'])
                    try:
                        for pair in pairs:
                            text = pair['text'].replace('：', '\t')
                            url = pair['url']
                            if len(text.split('\xa0')) == 2:
                                text = text.split('\xa0')[1]
                            cn, en = text.split('\t')
                            async with writer.transaction():
                                await writer.execute("INSERT INTO dictall_term (ch, en, url, category) VALUES ($1, $2, $3, $4)",
                                                           cn, en, url, category)
                    except BaseException as e:
                        print(e)


loop = asyncio.get_event_loop()

loop.run_until_complete(extract())

loop.close()





