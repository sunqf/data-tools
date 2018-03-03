#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import asyncio
import uvloop
import asyncpg
from corpus.util.config import headers
from bs4 import BeautifulSoup

from aiohttp import ClientSession
import traceback
from urllib.parse import urljoin
from bs4 import UnicodeDammit

from typing import Set

async def get_html(url, timeout=10, max_try_times=10):
    print(url)
    try_time = 0
    while max_try_times < 0 or try_time < max_try_times:
        try_time += 1
        try:
            async with ClientSession(headers=headers) as session:
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.read()
                        dammit = UnicodeDammit(data)
                        return dammit.unicode_markup
                    elif response.status == 404:
                        print('%s get 404' % url)
                        return None
                    elif response.status == 403:
                        print('%s get 403' % url)
                        return None
                    raise RuntimeError('raise for try again.')
        except UnicodeDecodeError as e:
            print(url, e)
            return None
        except Exception as e:
            print(url, e)
            traceback.print_exc()
            pass

    print('Getting %s exceed %d times.' % (url, try_time))
    return None


class Hudong:
    type = 'hudong_baike'
    search_address = 'http://so.baike.com'
    item_address = 'http://www.baike.com/wiki'

    def __init__(self, keywords: Set[str], finished_urls: Set[str], db_pool):
        self.keywords = keywords
        self.keyword_queue = asyncio.Queue()
        self.search_urls = set()
        self.item_queue = asyncio.Queue()
        self.finished_urls = finished_urls
        self.db_pool = db_pool

        self.search_num = 5
        self.item_num = 10
        self.save_num = 1

    def get_links(self, html):
        html = BeautifulSoup(html, 'html.parser')
        for tag in html.select('a[href]'):
            href = tag.attrs['href']
            if not href.startswith('javascript') and not href.startswith('#'):
                url = urljoin(self.item_address, href)
                url = url.split('#')[0]
                if self.item_address in url:
                    yield url

    async def search_work(self):
        async def _search(url):
            if url not in self.search_urls:
                html = await get_html(url)
                if html:
                    html = BeautifulSoup(html, 'html.parser')
                    for tag in html.select('#search-wiki > div > h3 > a'):
                        item_url = tag.attrs['href']
                        if self.item_address in item_url and item_url not in self.finished_urls:
                            await self.item_queue.put(item_url)
                            self.finished_urls.add(item_url)

                    self.search_urls.add(url)

                    for tag in html.select('#page > div > a'):
                        search_url = urljoin(self.search_address, tag.attrs['href'])
                        if self.search_address in search_url and search_url not in self.search_urls:
                            await _search(search_url)

        while True:
            keyword = await self.keyword_queue.get()
            try:
                url = 'http://so.baike.com/doc/{}&prd=button_doc_search'.format(keyword)
                await _search(url)

                async with self.db_pool.acquire() as writer:
                    async with writer.transaction():
                        await writer.executemany(
                            "INSERT INTO keywords (keyword, type) VALUES ($1, $2)",
                            [(keyword, self.type)])

            except Exception as e:
                print(url, e)

            self.keyword_queue.task_done()

    async def item_work(self):
        while True:
            item_url = await self.item_queue.get()
            try:
                item_html = await get_html(item_url)
                if item_html is not None:
                    async with self.db_pool.acquire() as writer:
                        async with writer.transaction():
                            await writer.executemany(
                                "INSERT INTO baike_html (html, url, type) VALUES ($1, $2, $3)",
                                [(item_html, item_url, self.type)])

                for new_url in self.get_links(item_html):
                    if new_url not in self.finished_urls:
                        await self.item_queue.put(new_url)
                        self.finished_urls.add(new_url)
            except Exception as e:
                print(item_url, e)
                traceback.print_exc()

            self.item_queue.task_done()

    async def run(self):
        for word in self.keywords:
            await self.keyword_queue.put(word)

        tasks = [asyncio.Task(self.search_work()) for i in range(self.search_num)] +\
                [asyncio.Task(self.item_work()) for i in range(self.item_num)]

        await self.keyword_queue.join()
        await self.item_queue.join()
        await self.html_queue.join()

        for task in tasks:
            task.cancel()

    @staticmethod
    async def create(path: str):
        keywords = set()
        with open(path) as file:
            for line in file:
                fields = line.split('\t')
                if len(fields) > 0:
                    word = fields[0].strip()
                    if len(word) > 0:
                        keywords.add(word)

        db_pool = await asyncpg.create_pool(host='localhost', user='sunqf', database='sunqf', command_timeout=60)

        finished_urls = set()
        async with db_pool.acquire() as db_conn:
            async with db_conn.transaction():
                records = await db_conn.fetch('SELECT keyword from keywords where type=\'{}\''.format(Hudong.type))
                finished_keywords = set([r['keyword'] for r in records])
                keywords.difference_update(finished_keywords)

            async with db_conn.transaction():
                records = await db_conn.fetch('SELECT url from baike_html where type=\'{}\''.format(Hudong.type))
                finished_urls = set([r['url'] for r in records])

        return Hudong(keywords, finished_urls, db_pool)


if __name__ == '__main__':
    import argparse
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dict', type=str, help='dict path')

    args = arg_parser.parse_args()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    crawler: Hudong = loop.run_until_complete(Hudong.create(args.dict))

    loop.run_until_complete(crawler.run())