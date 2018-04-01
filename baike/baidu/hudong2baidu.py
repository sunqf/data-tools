#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from .. import utils
from urllib.parse import urlparse, unquote
import asyncio
from pprint import pprint

async def get_hudong():
    db = await utils.connect()
    words = set()
    async with db.transaction():
        async for record in db.cursor('select url from baike_html where type=\'hudong_baike\''):
            url = record['url']
            parse_res = urlparse(url)
            item = unquote(parse_res.path.split('/')[2])
            words.add(item)

    return words

loop = asyncio.get_event_loop()
words = loop.run_until_complete(get_hudong())
pprint(words)
