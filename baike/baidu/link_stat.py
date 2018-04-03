#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from .. import utils
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from collections import Counter
import sys
import concurrent
import asyncio
from functools import reduce

async def stat(num_workers, worker_id):
    counter = Counter()
    db = await utils.connect()
    async with db.transaction():
        async for record in db.cursor(
                f'select url, html from baike_html where type=\'baidu_baike\' and id % {num_workers} = {worker_id} limit 10000'):
            url = record['url']
            html = utils.decompress(record['html'])
            tree = BeautifulSoup(html, 'html.parser')
            tree = utils.clean_tag(tree)
            content = tree.select_one('div.main-content')
            if content:
                links = [unquote(urljoin(url, href.attrs['href']))
                         for href in content.select('a[href]')
                         if not href.attrs['href'].startswith('#')]
                counter.update([link for link in links if link.startswith('https://baike.baidu.com/item')])

    return counter


def worker(num_workers, worker_id):
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(stat(num_workers, worker_id))


async def run(loop):
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        workers = [loop.run_in_executor(executor, worker, num_workers, id) for id in range(num_workers)]
        def _merge(x, y):
            x.update(y)
            return x
        return reduce(_merge, [await w for w in asyncio.as_completed(workers)])


num_workers = 10
loop = asyncio.get_event_loop()
counter = loop.run_until_complete(run(loop))

with open(sys.argv[1], 'w') as file:
    for k, v in counter.most_common():
        file.write('{}\t{}\n'.format(k, v))
