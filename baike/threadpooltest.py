#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-


import asyncio
import concurrent
import math


def func():
    for i in range(10000000):
        math.sqrt(i)


loop = asyncio.get_event_loop()


async def run():
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        await asyncio.gather([loop.run_in_executor(executor, func) for t in range(100)])

loop.run_until_complete(run())
