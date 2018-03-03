#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-


import asyncio
import asyncpg
import re

_sep = re.compile('[;,：，/ ]')


async def build():

    terms = set()

    db_conn = await asyncpg.connect(host='localhost', user='sunqf', database='sunqf', command_timeout=60)

    async with db_conn.transaction():
        records = await db_conn.fetch('SELECT ch, en from dictall_term')
        for record in records:
            chs = _sep.split(record['ch'])
            ens = _sep.split(record['en'])
            terms.update(chs)
            terms.update(ens)
            terms.add(record['en'])

    for term in terms:
        print(term)


loop = asyncio.get_event_loop()
loop.run_until_complete(build())
loop.close()


