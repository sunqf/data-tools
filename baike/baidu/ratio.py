#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import sys
from .. import utils
import asyncio
from tqdm import tqdm


async def get_knowledge(data):
    data = dict(data)
    db = await utils.connect()
    has_result = []
    no_result = []
    async with db.transaction():
        async for record in db.cursor('SELECT url, knowledge FROM baike_knowledge where type = \'baidu_baike\''):
            url = record['url']
            knowledge = record['knowledge']
            if url in data:
                has_result.append((url, data[url], knowledge))
                del data[url]
    return sorted(has_result, key=lambda x: x[1], reverse=True), sorted(data.items(), key=lambda x: x[1], reverse=True)

counter = []
total = 0
with open(sys.argv[1]) as file:

    for line in file:
        word, count = line.rsplit('\t', maxsplit=1)
        count = int(count)
        counter.append((word, count))
        total += count

loop = asyncio.get_event_loop()
has_res, no_res = loop.run_until_complete(get_knowledge(counter))

acc = 0
with open(sys.argv[2]+'.has', 'w') as writer:
    for word, count, knowledge in has_res:
        acc += count
        writer.write(f'{word}\t{count}\t{acc/total}\t{knowledge}\n')

acc = 0
with open(sys.argv[2]+'.no', 'w') as writer:
    for word, count in has_res:
        acc += count
        writer.write(f'{word}\t{count}\t{acc/total}\n')