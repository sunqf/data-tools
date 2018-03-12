#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import asyncio
import functools
import time
import traceback
from urllib.parse import urlparse, urljoin
import zlib
import asyncpg
import uvloop
from aiohttp import ClientSession
from bs4 import BeautifulSoup, UnicodeDammit, Tag
from typing import Set, List
import concurrent
from corpus.util.config import headers
from urllib.parse import unquote


def statistics(func):
    class Info:
        def __init__(self):
            self.count = 0
            self.data_size = 0
            self.start_time = None
            self.interval = 500

        def incr(self) -> bool:
            if self.start_time is None:
                self.start_time = time.time()
            self.count += 1
            return self.count % self.interval == 0

        def speed(self) -> float:
            elapsed_time = time.time() - self.start_time
            return self.count/elapsed_time

    info = Info()

    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        res = await func(*args, **kwargs)
        if info.incr():
            print('{} {} requests per second'.format(func, info.speed()))
        return res
    return wrapped


@statistics
async def get_html(session, url, timeout=10, max_try_times=10):
    print(url)
    try_time = 0
    while max_try_times < 0 or try_time < max_try_times:
        try_time += 1
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    data = await response.read()
                    dammit = UnicodeDammit(data)
                    return str(response.url), dammit.unicode_markup
                elif response.status == 404:
                    print('%s get 404' % url)
                    return str(response.url), None
                elif response.status == 403:
                    print('%s get 403' % url)
                    return str(response.url), None
                raise RuntimeError('raise for try again.')
        except UnicodeDecodeError as e:
            print(url, e)
            return url, None
        except Exception as e:
            print(url, e)
            traceback.print_exc()
            pass

    print('Getting %s exceed %d times.' % (url, try_time))
    return url, None


def compress(html: str):
    return zlib.compress(html.encode())


def uncompress(data: bytes):
    return zlib.decompress(data).decode()


def decompose(html: Tag, selectors: List[str]) -> Tag:
    for selector in selectors:
        for tag in html.select(selector):
            tag.decompose()
    return html


def extract_links(url, html: Tag) -> Set[str]:
    if html is not None:
        parse_result = urlparse(url)
        host = '{}://{}'.format(parse_result.scheme, parse_result.netloc)
        return set(urljoin(host, a.attrs['href']) for a in html.select('a[href]'))
    else:
        return set()


def compress_func(batch, selectors: List[str]):
    res = []
    links = set()
    for url, html in batch:
        try:
            html = BeautifulSoup(html, 'html.parser')
            links.update(extract_links(url, html))
            html = decompose(html, selectors)
            res.append((url, compress(str(html))))
        except Exception as e:
            print(url, e)
            traceback.print_exc()

    return res

class Queue:
    def __init__(self, priority=True):
        self.queue = asyncio.PriorityQueue() if priority else asyncio.Queue()
        self.processing = set()

    async def get(self):
        return await self.queue.get()

    async def put(self, task):
        if task not in self.processing:
            self.processing.add(task)
            await self.queue.put(task)
            return True
        else:
            return False

    def task_done(self, task):
        self.queue.task_done()
        self.processing.remove(task)

    async def join(self):
        await self.queue.join()


class BaseCrawler:
    def __init__(self, type, num_workers, decompose_selectors):
        self.type = type
        self.num_worker = num_workers
        self.decompose_selectors = decompose_selectors
        self.url_queue = Queue(priority=True)
        self.search_order = 2
        self.item_order = 1
        self.html_queue = asyncio.Queue()
        self.finished_urls = set()

        self.client_session = None

    def format_keyword(self, word: str) -> str:
        raise NotImplementedError()

    def is_item_url(self, url: str) -> bool:
        raise NotImplementedError()

    def is_search_url(self, url: str) -> bool:
        raise NotImplementedError()

    def format_url(self, url: str) -> str:
        return unquote(url.split('#')[0].split('?')[0] if self.is_item_url(url) else url)

    async def add_new_links(self, url: str, html: Tag):
        for new_url in extract_links(url, html):
            new_url = self.format_url(new_url)
            if new_url not in self.finished_urls:
                if self.is_item_url(new_url):
                    await self.url_queue.put((self.item_order, new_url))
                elif self.is_search_url(new_url):
                    await self.url_queue.put((self.search_order, new_url))

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
                    response_url, html = await get_html(self.client_session, url)
                    response_url = self.format_url(response_url)
                    if response_url not in self.finished_urls and html:
                        self.finished_urls.add(url)
                        self.finished_urls.add(response_url)
                        await self.html_queue.put((response_url, html))
                except Exception as e:
                    print(url, e)
                    traceback.print_exc()
            self.url_queue.task_done((order, url))

    @staticmethod
    async def get_batches(queue: asyncio.Queue, batch_size: int):
        for i in range(batch_size):
            if i == 0:
                yield await queue.get()
                queue.task_done()
            else:
                try:
                    yield queue.get_nowait()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    return

    async def save_html_worker(self):
        writer = await self.db_connect()
        while True:
            try:
                urls = []
                data = []
                async for url, html in self.get_batches(self.html_queue, 10):
                    urls.append(url)
                    tree = BeautifulSoup(html, 'html.parser')
                    await self.add_new_links(url, tree)
                    if self.is_item_url(url):
                        tree = decompose(tree, self.decompose_selectors)
                        data.append((url, compress(str(tree))))

                async with writer.transaction():
                    await writer.executemany(
                        "INSERT INTO finished_url (url, type) VALUES ($1, $2)",
                        [(url, self.type) for url in urls])

                if len(data) > 0:
                    async with writer.transaction():
                        await writer.executemany(
                            "INSERT INTO baike_html2 (html, url, type) VALUES ($1, $2, $3)",
                            [(html, url, self.type) for url, html in data])

            except Exception as e:
                print(e)
                traceback.print_exc()

    async def run(self, dict_path: str):
        self.client_session = ClientSession(headers=headers)
        await self.get_finished()

        source = asyncio.ensure_future(self.put_keywords(dict_path))
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            tasks = [asyncio.ensure_future(self.save_html_worker()),
                     *[asyncio.ensure_future(self.crawl_worker()) for _ in range(self.num_worker)]]

            await source
            await self.url_queue.join()
            await self.html_queue.join()

            for task in tasks:
                task.cancel()

    @staticmethod
    async def db_connect():
        return await asyncpg.connect(host='localhost', user='sunqf', password='840422', database='sunqf', command_timeout=60)

    async def get_finished(self):
        reader = await self.db_connect()
        async with reader.transaction():
            records = await reader.fetch('SELECT url from finished_url where type=\'{}\''.format(self.type))
            self.finished_urls.update([r['url'] for r in records])

        async with reader.transaction():
            records = await reader.fetch('SELECT url from baike_html2 where type=\'{}\''.format(self.type))
            self.finished_urls.update([r['url'] for r in records])

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
    decompose_selectors = ['script', 'link', 'div.header-wrapper', 'div.navbar-wrapper',
                           'div.wgt-footer-main', 'div.lemmaWgt-searchHeader', 'div.after-content'
                           'div#side-share', 'div#layer', 'div.right-ad']

    def __init__(self):
        super(Baidu, self).__init__('baidu_baike', num_workers=10, decompose_selectors=self.decompose_selectors)

    def format_keyword(self, word: str) -> str:
        return 'https://baike.baidu.com/search?word={}&pn=0&rn=0&enc=utf8'.format(word)

    def is_item_url(self, url: str) -> bool:
        return url.startswith('https://baike.baidu.com/item')

    def is_search_url(self, url: str):
        return url.startswith('https://baike.baidu.com/search')

    async def convert(self):
        loop = asyncio.get_event_loop()

        batches = asyncio.Queue(maxsize=1000)
        async def get_batches():
            reader = await asyncpg.connect(host='localhost', user='sunqf', password='840422', database='sunqf',
                                           command_timeout=60)
            async with reader.transaction():
                batch = []
                async for record in reader.cursor(
                        'SELECT url, html, type from baike_html where type=\'{}\' and url NOT IN (SELECT url FROM baike_html2)'.format(
                            self.type), prefetch=200, timeout=1200):
                    url = record['url']
                    html = record['html']
                    type = record['type']
                    batch.append((url, html))
                    if len(batch) > 10:
                        await batches.put(batch)
                        batch = []

                if len(batch) > 0:
                    await batches.put(batch)

        async def compress_worker(executor):
            writer = await asyncpg.connect(host='192.168.0.11', user='sunqf', password='840422', database='sunqf',
                                           command_timeout=60)
            while True:
                batch = await batches.get()
                comp_batch = await loop.run_in_executor(executor, compress_func, batch, self.decompose_selectors)
                async with writer.transaction():
                    await writer.executemany("INSERT INTO baike_html2 (html, url, type) VALUES ($1, $2, $3)  ON CONFLICT DO NOTHING",
                                             [(html, url, self.type) for url, html in comp_batch])
                batches.task_done()

        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
            source = asyncio.ensure_future(get_batches())
            tasks = [asyncio.ensure_future(compress_worker(executor)) for i in range(10)]

            await source
            for task in tasks:
                task.cancel()

        '''
        async with reader.transaction():
            async for record in reader.cursor(
                    'SELECT url, html, type from baike_html where type=\'{}\' and url NOT IN (SELECT url FROM baike_html2)'.format(self.type)):
                url = record['url']
                html = record['html']
                type = record['type']

                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                    try:
                        html = BeautifulSoup(html, 'html.parser')
                        html = self.decompose(html)

                        async with writer.transaction():
                            await writer.executemany("INSERT INTO baike_html2 (html, url, type) VALUES ($1, $2, $3)",
                                            [(self.compress(str(html)), url, self.type)])
                    except Exception as e:
                        print(url, e)
                        traceback.print_exc()

        '''
class Hudong(BaseCrawler):
    decompose_selector = ['script', 'link', 'div.header-baike', 'div.header-search',
                          'div.point.l-he22.descriptionP', 'iframe', 'div.bklog', 'div.mainnav-wrap',
                          'div#renzheng', 'div.dialog_HDpopMsg'
                        ]
    def __init__(self):
        super(Hudong, self).__init__('hudong_baike',
                                     num_workers=10,
                                     decompose_selectors=self.decompose_selectors)

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
        #loop.run_until_complete(crawler.run(args.dict))
        loop.run_until_complete(crawler.convert())
    elif args.type == 'hudong':
        crawler = Hudong()
        loop.run_until_complete(crawler.run(args.dict))

