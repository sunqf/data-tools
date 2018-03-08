#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import asyncio
import asyncpg
from corpus.util.config import headers
from bs4 import BeautifulSoup

from aiohttp import ClientSession, TCPConnector
import traceback
from urllib.parse import urljoin
from bs4 import UnicodeDammit
import functools
from collections import namedtuple
import time
import logging

def statistics(func):
    class Info:
        def __init__(self):
            self.count = 0
            self.start_time = 0.
            self.interval = 500

        def incr(self):
            self.count += 1
            return self.count == self.interval

        def speed(self):
            speed = self.interval/(time.time() - self.start_time)
            self.start_time = time.time()
            self.count = 0

            return speed

    info = Info()

    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        res = await func(*args, **kwargs)

        if info.incr():
            print('{} speed: {} per second'.format(func, info.speed()))

        return res
    return wrapped


@statistics
async def get_html(url, timeout=10, max_try_times=10):
    print(url)
    try_time = 0
    while max_try_times < 0 or try_time < max_try_times:
        try_time += 1
        try:
            connector = TCPConnector(limit=50)
            async with ClientSession(connector=connector, headers=headers) as session:
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


class BaseCrawler:
    def __init__(self, type,
                 search_address, search_template, search_worker_num,
                 item_selector, page_selector,
                 item_address, item_worker_num,
                 loop):
        self.type = type
        self.search_address = search_address
        self.search_template = search_template
        self.item_selector = item_selector
        self.page_selector = page_selector

        self.item_address = item_address

        self.search_worker_num = search_worker_num
        self.item_worker_num = item_worker_num

        self.keyword_queue = asyncio.Queue()
        self.search_urls = set()
        self.item_queue = asyncio.Queue()

        self.db_pool, self.finished_urls = loop.run_until_complete(self.init())

    async def init(self):
        # db_pool = await asyncpg.create_pool(host='localhost', user='sunqf', database='sunqf', command_timeout=60)
        db_pool = await asyncpg.create_pool(host='localhost', user='sunqf', database='sunqf', command_timeout=60)

        async with db_pool.acquire() as writer:
            async with writer.transaction():
                records = await writer.fetch('SELECT url from baike_html where type=\'{}\''.format(self.type))
                finished_urls = set([r['url'] for r in records])
        return db_pool, finished_urls

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
                    for tag in html.select(self.item_selector):
                        item_url = tag.attrs['href']
                        if self.item_address in item_url and item_url not in self.finished_urls:
                            await self.item_queue.put(item_url)
                            self.finished_urls.add(item_url)

                    self.search_urls.add(url)

                    if self.page_selector is not None:
                        for tag in html.select(self.page_selector):
                            search_url = urljoin(self.search_address, tag.attrs['href'])
                            if self.search_address in search_url and search_url not in self.search_urls:
                                await _search(search_url)

        while True:
            keyword = await self.keyword_queue.get()
            try:
                url = self.search_template.format(keyword)
                await _search(url)

                async with self.db_pool.acquire() as writer:
                    async with writer.transaction():
                        await writer.executemany(
                            "INSERT INTO keywords (keyword, type) VALUES ($1, $2)",
                            [(keyword, self.type)])

            except Exception as e:
                print(url, e)
                traceback.print_exc()

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

    async def run(self, dict_path: str):
        for word in await self.get_keywords(dict_path):
            await self.keyword_queue.put(word)

        tasks = [asyncio.Task(self.search_work()) for i in range(self.search_worker_num)] + \
                [asyncio.Task(self.item_work()) for i in range(self.item_worker_num)]

        await self.keyword_queue.join()
        await self.item_queue.join()

        for task in tasks:
            task.cancel()

    async def get_keywords(self, path: str):
        keywords = set()
        with open(path) as file:
            for line in file:
                fields = line.split('\t')
                if len(fields) > 0:
                    word = fields[0].strip()
                    if len(word) > 0:
                        keywords.add(word)

        async with self.db_pool.acquire() as reader:
            async with reader.transaction():
                records = await reader.fetch('SELECT keyword from keywords where type=\'{}\''.format(self.type))
                finished_keywords = set([r['keyword'] for r in records])
                keywords.difference_update(finished_keywords)

        return keywords
