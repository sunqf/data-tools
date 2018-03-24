#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from .. import utils
from bs4 import BeautifulSoup
import asyncio


async def extract_relation_table():
    db = await utils.connect()
    async with db.transaction():
        async for record in db.cursor('select url, html from baike_html2 where type=\'baidu_baike\''):
            url = record['url']
            html = utils.decompress(record['html'])
            tree = BeautifulSoup(html, 'html.parser')
            for table in tree.select('div.lemma-relation-table'):
                print(table)

loop = asyncio.get_event_loop()
loop.run_until_complete(extract_relation_table())