#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from .tasks import named_url
import asyncio
from . import utils
import concurrent
from pprint import pprint
import traceback

queue = asyncio.Queue(maxsize=10000)

async def producer(loop):
    db = await utils.connect()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        async with db.transaction():
            async for record in db.cursor('select url, knowledge from baike_knowledge2 where type=\'baidu_baike\' limit 10000'):
                url = record['url']
                knowledge = record['knowledge']
                result = named_url.delay(url, knowledge)
                # future = loop.run_in_executor(executor, async_get, url, knowledge)
                await queue.put(result)
    await queue.put(None)


async def collect():
    while True:
        future = await queue.get()
        if future is None:
            break
        # result = await future
        # print(result)
        result = future.get(interval=1)
        if result:
            url, names = result
            if len(names) == 1:
                yield url, names[0]


async def run(loop):
    future = asyncio.ensure_future(producer(loop))
    return [(url, name) async for url, name in collect()]


loop = asyncio.get_event_loop()
url2name = loop.run_until_complete(run(loop))
