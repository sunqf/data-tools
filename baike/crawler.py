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

ITEM_ORDER = 1
SEARCH_ORDER = 2


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


def uncompress(data):
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


def extract_and_compress(data: list,
                         item_prefix: str,
                         search_prefix: str,
                         decompose_selectors: List[str]):
    save_urls = []
    save_htmls = []
    new_urls = set()
    for url, html in data:
        save_urls.append(url)
        if html:
            try:
                tree = BeautifulSoup(html, 'html.parser')
                for new_url in extract_links(url, tree):
                    if new_url.startswith(item_prefix):
                        new_urls.add((ITEM_ORDER, new_url))
                    elif new_url.startswith(search_prefix):
                        new_urls.add((SEARCH_ORDER, new_url))

                if url.startswith(item_prefix):
                    tree = decompose(tree, decompose_selectors)
                    save_htmls.append((url, compress(str(tree))))
            except Exception as e:
                print(url, e)
                traceback.print_exc()

    return save_urls, save_htmls, new_urls


def uncompress_and_extract(data: list):
    new_urls = set()
    for url, html in data:
        try:
            html = BeautifulSoup(uncompress(html), 'html.parser')
            new_urls.update(extract_links(url, html))
        except Exception as e:
            print(url, e)
            traceback.print_exc()

    return new_urls


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

    def qsize(self):
        return self.queue.qsize()

    def task_done(self, task):
        self.queue.task_done()
        self.processing.remove(task)

    async def join(self):
        await self.queue.join()


class BaseCrawler:
    def __init__(self, type, keyword_format, search_prefix, item_prefix, num_workers, decompose_selectors):
        self.type = type
        self.keyword_format = keyword_format
        self.search_prefix = search_prefix
        self.item_prefix = item_prefix
        self.num_worker = num_workers
        self.decompose_selectors = decompose_selectors
        self.url_queue = Queue(priority=True)
        self.html_queue = asyncio.Queue(maxsize=10000)
        self.save_queue = asyncio.Queue(maxsize=10000)
        self.finished_urls = set()

        self.client_session = None

    def format_keyword(self, word: str) -> str:
        return self.keyword_format.format(word)

    def is_item_url(self, url: str) -> bool:
        return url.startswith(self.item_prefix)

    def is_search_url(self, url: str) -> bool:
        return url.startswith(self.search_prefix)

    def format_url(self, url: str) -> str:
        return unquote(url.split('#')[0].split('?')[0] if self.is_item_url(url) else url)

    async def put_keywords(self, dict_path):
        for word in self.get_keywords(dict_path):
            url = self.format_keyword(word)
            if url not in self.finished_urls:
                await self.url_queue.put((SEARCH_ORDER, url))

    async def crawl_worker(self):
        while True:
            order, url = await self.url_queue.get()
            if url not in self.finished_urls:
                try:
                    response_url, html = await get_html(self.client_session, url)
                    response_url = self.format_url(response_url)
                    if html:
                        self.finished_urls.add(url)
                        if url == response_url:
                            await self.html_queue.put((url, html))
                        else:
                            await self.html_queue.put((url, None))
                            if response_url not in self.finished_urls:
                                self.finished_urls.add(response_url)
                                await self.html_queue.put((response_url, html))
                except Exception as e:
                    print(url, e)
                    traceback.print_exc()
            self.url_queue.task_done((order, url))

    @staticmethod
    async def get_batches(queue: asyncio.Queue, batch_size: int):
        batch = []
        for i in range(batch_size):
            if i == 0:
                batch.append(await queue.get())
                queue.task_done()
            else:
                try:
                    batch.append(queue.get_nowait())
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break

        return batch

    async def compress_worker(self, loop, executor):
        while True:
            try:
                batch = await self.get_batches(self.html_queue, 20)
                future = loop.run_in_executor(executor,
                                              extract_and_compress,
                                              batch, self.item_prefix, self.search_prefix, self.decompose_selectors)
                await self.save_queue.put(future)
            except Exception as e:
                print(e)
                traceback.print_exc()

    async def save_worker(self):
        writer = await self.db_connect()
        while True:
            try:
                future = await self.save_queue.get()
                save_urls, save_htmls, new_urls = await future
                for order, url in new_urls:
                    url = self.format_url(url)
                    if url not in self.finished_urls:
                        await self.url_queue.put((order, url))

                if len(save_htmls) > 0:
                    async with writer.transaction():
                        await writer.executemany(
                            "INSERT INTO baike_html2 (html, url, type) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                            [(html, url, self.type) for url, html in save_htmls])

                async with writer.transaction():
                    await writer.executemany(
                        "INSERT INTO finished_url (url, type) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                        [(url, self.type) for url in save_urls])



            except Exception as e:
                print(e)
                traceback.print_exc()

            self.save_queue.task_done()

    async def run(self, dict_path: str, loop, num_worker):
        self.client_session = ClientSession(headers=headers)
        await self.get_finished()

        with concurrent.futures.ProcessPoolExecutor(max_workers=num_worker) as executor:
            await asyncio.ensure_future(self.get_lost(loop, executor))
            source = asyncio.ensure_future(self.put_keywords(dict_path))

            tasks = [asyncio.ensure_future(self.compress_worker(loop, executor)),
                     asyncio.ensure_future(self.save_worker()),
                     *[asyncio.ensure_future(self.crawl_worker()) for _ in range(self.num_worker)]]

            await source
            await self.url_queue.join()
            await self.html_queue.join()
            await self.save_queue.join()
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

    async def get_lost(self, loop, executor):
        reader = await self.db_connect()
        futures = []

        async with reader.transaction():
            batch = []
            async for record in reader.cursor('SELECT url, html from baike_html2 where type=\'{}\''.format(self.type)):
                url = record['url']
                html = record['html']
                batch.append((url, html))
                if len(batch) > 200:
                    futures.append(loop.run_in_executor(executor, uncompress_and_extract, batch))
                    batch = []

                if len(futures) > 100:
                    for future in asyncio.as_completed(futures):
                        for new_url in await future:
                            new_url = self.format_url(new_url)
                            if new_url not in self.finished_urls:
                                if self.is_item_url(new_url):
                                    await self.url_queue.put((ITEM_ORDER, new_url))
                                elif self.is_search_url(new_url):
                                    await self.url_queue.put((SEARCH_ORDER, new_url))
                    print(self.url_queue.qsize())
                    futures = []

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
        super(Baidu, self).__init__(type='baidu_baike',
                                    keyword_format='https://baike.baidu.com/search?word={}&pn=0&rn=0&enc=utf8',
                                    search_prefix='https://baike.baidu.com',
                                    item_prefix='https://baike.baidu.com/item',
                                    num_workers=5, decompose_selectors=self.decompose_selectors)


class Hudong(BaseCrawler):
    decompose_selectors = ['script', 'link', 'div.header-baike', 'div.header-search',
                           'div.point.l-he22.descriptionP', 'iframe', 'div.bklog', 'div.mainnav-wrap',
                           'div#renzheng', 'div.dialog_HDpopMsg'
                        ]
    def __init__(self):
        super(Hudong, self).__init__(type='hudong_baike',
                                     keyword_format='http://so.baike.com/doc/{}&prd=button_doc_search',
                                     search_prefix='http://so.baike.com',
                                     item_prefix='http://www.baike.com/wiki',
                                     num_workers=5,
                                     decompose_selectors=self.decompose_selectors)



'''
TODO:  360和sogou有限制，走代理可能可以解决。

class Qihu(BaseCrawler):
    decompose_selectors = ['']
    def __init__(self):
        super(Qihu, self).__init__('360_baike', num_workers=10, decompose_selectors=self.decompose_selectors)
    
    def format_keyword(self, word: str) -> str:
        return 'https://baike.so.com/search/?q={}'.format(word)
    
    def is_item_url(self, url: str):
        return url.startswith('https://baike.so.com/doc')
    
    def is_search_url(self, url: str):
        return url.startswith('https://baike.so.com/search')
    

class Sogou(BaseCrawler):
    decompose_selectors = ['']
    def __init__(self):
        super(Sogou, self).__init__('sogou_baike', num_workers=10, decompose_selectors=self.decompose_selectors)

    def format_keyword(self, word: str) -> str:
        return 'http://baike.sogou.com/Search.e?sp=S{}&sp=0'.format(word)

    def is_item_url(self, url: str):
        return url.startswith('http://baike.sogou.com/v')

    def is_search_url(self, url: str):
        return url.startswith('http://baike.sogou.com/Search')


# 医学百科
# https://www.wiki8.com
# http://baike.molbase.cn/c1/
'''

if __name__ == '__main__':
    import argparse
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dict', type=str, help='dict path')
    arg_parser.add_argument('--type', type=str, help='baike type')
    arg_parser.add_argument('--num-worker', type=int, default=5, help='num executor')

    args = arg_parser.parse_args()

    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    if args.type == 'baidu':
        crawler = Baidu()
        loop.run_until_complete(crawler.run(args.dict, loop, args.num_worker))
        # loop.run_until_complete(crawler.convert())
    elif args.type == 'hudong':
        crawler = Hudong()
        loop.run_until_complete(crawler.run(args.dict, loop, args.num_worker))

