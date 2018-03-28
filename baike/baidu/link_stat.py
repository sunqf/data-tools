#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from .. import utils
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from collections import Counter
import sys

async def stat():
    counter = Counter()
    db = await utils.connect()
    async with db.transaction():
        async for record in db.cursor('select url, html from baike_html2 where type=\'baidu_baike\''):
            url = record['url']
            html = utils.decompress(record['html'])
            tree = BeautifulSoup(html, 'html.parser')
            tree = utils.clean_tag(tree)
            counter.update(unquote(urljoin(url, href.attrs['href']))
                           for href in tree.select('a[href]') if not href.attrs['href'].startswith('#'))

    return counter


import asyncio

loop = asyncio.get_event_loop()
counter = loop.run_until_complete(stat())

with open(sys.argv[1], 'w') as file:
    for k, v in counter.most_common():
        file.write('{}\t{}\n'.format(k, v))
