#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-


import asyncio
import asyncpg
import re
from corpus.bicorpus import db

_sep = re.compile('[;,：，/ ]')


async def build():

    terms = set()

    db_conn = await db.connect()

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


