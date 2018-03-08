#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import asyncio
import functools
import time
import traceback
from urllib.parse import urlparse, urljoin

import asyncpg
import uvloop
from aiohttp import ClientSession, TCPConnector
from bs4 import BeautifulSoup, UnicodeDammit


from corpus.util.config import headers


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
    def __init__(self, type, num_workers):
        self.type = type
        self.num_worker = num_workers
        self.url_queue = asyncio.PriorityQueue()
        self.search_order = 2
        self.item_order = 1
        self.save_html_queue = asyncio.Queue()
        self.save_url_queue = asyncio.Queue()
        self.finished_urls = set()

    def format_keyword(self, word: str) -> str:
        raise NotImplementedError()

    def is_item_url(self, url: str) -> bool:
        raise NotImplementedError()

    def is_search_url(self, url: str) -> bool:
        raise NotImplementedError()

    def format_url(self, url: str) -> str:
        return url.split('#')[0].split('?')[0] if self.is_item_url(url) else url

    @staticmethod
    def extract_links(url, html):
        parse_result = urlparse(url)
        host = '{}://{}'.format(parse_result.scheme, parse_result.netloc)
        if html is not None:
            html = BeautifulSoup(html, 'html.parser')
            return set(urljoin(host, a.attrs['href']) for a in html.select('a[href]'))

    async def put_keywords(self, dict_path):
        for word in self.get_keywords(dict_path):
            url = self.format_keyword(word)
            if url not in self.finished_urls:
                await self.url_queue.put((self.search_order, url))

    async def crawl_worker(self):
        while True:
            order, url = await self.url_queue.get()
            if url not in self.finished_urls:
                try:
                    self.finished_urls.add(url)
                    html = await get_html(url)
                    if html is not None:
                        if self.is_item_url(url):
                            await self.save_html_queue.put((url, html))
                        await self.save_url_queue.put(url)

                        for new_url in self.extract_links(url, html):
                            if new_url not in self.finished_urls:
                                if self.is_item_url(new_url):
                                    await self.url_queue.put((self.item_order, self.format_url(new_url)))
                                elif self.is_search_url(new_url):
                                    await self.url_queue.put((self.search_order, new_url))

                except Exception as e:
                    print(url, e)
                    traceback.print_exc()

            self.url_queue.task_done()

    async def save_urls_worker(self):
        writer = await self.db_connect()
        while True:
            data = []
            url = await self.save_url_queue.get()
            data.append((url))
            for i in range(50):
                try:
                    data.append(self.save_url_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if len(data) > 0:
                async with writer.transaction():
                    await writer.executemany(
                        "INSERT INTO finished_url (url, type) VALUES ($1, $2)",
                        [(url, self.type) for url in data])

    async def save_html_worker(self):
        writer = await self.db_connect()
        while True:
            data = []
            url, html = await self.save_html_queue.get()
            data.append((url, html))
            for i in range(50):
                try:
                    data.append(self.save_html_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if len(data) > 0:
                async with writer.transaction():
                    await writer.executemany(
                        "INSERT INTO baike_html (html, url, type) VALUES ($1, $2, $3)",
                        [(html, url, self.type) for url, html in data])

    async def run(self, dict_path: str):
        self.finished_urls = await self.get_finished()

        await self.put_keywords(dict_path)
        tasks = [asyncio.ensure_future(self.save_html_worker()),
                 asyncio.ensure_future(self.save_urls_worker()),
                 *[asyncio.ensure_future(self.crawl_worker()) for i in range(self.num_worker)]]

        await self.url_queue.join()
        await self.save_html_queue.join()
        await self.save_url_queue.join()

        for task in tasks:
            task.cancel()

    @staticmethod
    async def db_connect():
        return await asyncpg.connect(host='localhost', user='sunqf', password='840422', database='sunqf', command_timeout=60)

    async def get_finished(self):
        writer = await self.db_connect()
        async with writer.transaction():
            records = await writer.fetch('SELECT url from finished_url where type=\'{}\''.format(self.type))
            finished_urls = set([r['url'] for r in records])
        return finished_urls

    @staticmethod
    def get_keywords(path: str):
        keywords = set()
        with open(path) as file:
            for line in file:
                fields = line.split('\t')
                if len(fields) > 0:
                    word = fields[0].strip()
                    if len(word) > 0:
                        keywords.add(word)

        return keywords


class Baidu(BaseCrawler):
    def __init__(self):
        super(Baidu, self).__init__('baidu_baike', num_workers=10)

    def format_keyword(self, word) -> str:
        return 'https://baike.baidu.com/search?word={}&pn=0&rn=0&enc=utf8'.format(word)

    def is_item_url(self, url) -> bool:
        return url.startswith('https://baike.baidu.com/item')

    def is_search_url(self, url):
        return url.startswith('https://baike.baidu.com/search')


class Hudong(BaseCrawler):
    def __init__(self):
        super(Hudong, self).__init__('hudong_baike', num_workers=10)

    def format_keyword(self, word) -> str:
        return 'http://so.baike.com/doc/{}&prd=button_doc_search'.format(word)

    def is_item_url(self, url) -> bool:
        return url.startswith('http://www.baike.com/wiki')

    def is_search_url(self, url) -> bool:
        return url.startswith('http://so.baike.com')


if __name__ == '__main__':
    import argparse
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dict', type=str, help='dict path')
    arg_parser.add_argument('--type', type=str, help='baike type')

    args = arg_parser.parse_args()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    if args.type == 'baidu':
        crawler = Baidu()
        loop.run_until_complete(crawler.run(args.dict))
    elif args.type == 'hudong':
        crawler = Hudong()
        loop.run_until_complete(crawler.run(args.dict))

