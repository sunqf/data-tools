#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from .. import utils
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from collections import Counter
from pprint import pprint
async def stat():
    counter = Counter()
    db = await utils.connect()
    async with db.transaction():
        async for record in db.cursor('select url, html from baike_html2 where type=\'baidu_baike\''):
            url = record['url']
            html = utils.decompress(record['html'])
            tree = BeautifulSoup(html, 'html.parser')
            tree = utils.clean_tag(tree)
            counter.update(urljoin(url, href.attrs['href']) for href in tree.select('a[href]'))

    return counter


import asyncio

loop = asyncio.get_event_loop()
counter = loop.run_until_complete(stat())
pprint(counter.most_common())
