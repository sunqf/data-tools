#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import asyncpg
import posixpath
from logging import *
from corpus.util.config import *
from corpus.util.headless import Headless
from typing import Set, Optional, AsyncGenerator
from bs4 import UnicodeDammit, BeautifulSoup
import re
from urllib import robotparser, parse

getLogger().setLevel(INFO)


# common file extensions that are not followed if they occur in links
# copy from https://github.com/scrapy/scrapy/blob/master/scrapy/linkextractors/__init__.py
IGNORED_EXTENSIONS = [
    # images
    'mng', 'pct', 'bmp', 'gif', 'jpg', 'jpeg', 'png', 'pst', 'psp', 'tif',
    'tiff', 'ai', 'drw', 'dxf', 'eps', 'ps', 'svg',

    # audio
    'mp3', 'wma', 'ogg', 'wav', 'ra', 'aac', 'mid', 'au', 'aiff',

    # video
    '3gp', 'asf', 'asx', 'avi', 'mov', 'mp4', 'mpg', 'qt', 'rm', 'swf', 'wmv',
    'm4a', 'm4v',

    # office suites
    'xls', 'xlsx', 'ppt', 'pptx', 'pps', 'doc', 'docx', 'odt', 'ods', 'odg',
    'odp',

    # other
    'css', 'pdf', 'exe', 'bin', 'rss', 'zip', 'rar',
]

IGNORED_EXTENSIONS = ['.' + e for e in IGNORED_EXTENSIONS]


def url_has_any_extension(url: str, extensions: Set[str]):
    return posixpath.splitext(parse.urlparse(url).path)[1].lower() in extensions


class BaseCrawler:

    def __init__(self, type, host):
        self.type = type
        self.host = host

        self.robot_parser = self.init_robot_parser(host)

        self.page_pattern = re.compile('^[ ]*(上一页|下一页|尾页)[ ]*$')

    @staticmethod
    def init_robot_parser(host):
        try:
            robot_parser = robotparser.RobotFileParser()
            robot_parser.set_url(urljoin(host, 'robots.txt'))
            robot_parser.read()
            return robot_parser
        except Exception as e:
            log(WARNING, e)

        return None

    async def get_urls(self):
        raise NotImplementedError

    async def get_html(self, url) -> Optional[str]:
        raise NotImplementedError

    def select_multi_page(self, url, content: BeautifulSoup):
        page_text = content.find(text=self.page_pattern)
        if page_text and page_text.parent and page_text.parent.parent:
            for page_a in page_text.parent.parent.select('a[href]'):
                page_href = page_a.attrs['href']
                if not page_href.startswith('javascript'):
                    new_url = urljoin(url, page_href)
                    yield new_url

    async def crawl(self, finished: Set[str]) -> AsyncGenerator[(str, str)]:
        async for url in self.get_urls():
            if url not in finished:
                content = await self.get_html(url)
                if content:
                    yield url, content
                    finished.add(url)

                    # 处理多页的问题
                    content = BeautifulSoup(content, 'html.parser')
                    for new_url in self.select_multi_page(url, content):
                        if new_url not in finished:
                            new_content = await self.get_html(new_url)
                            if new_content:
                                yield new_url, new_content
                                finished.add(new_url)
                else:
                    log(WARNING, 'Can\'t get %s' % url)

    async def save(self, db_conn, buffer):
        try:
            async with db_conn.transaction():
                await db_conn.executemany("INSERT INTO raw_html (url, html, type) VALUES ($1, $2, $3)",
                                          [(url, html, self.type) for url, html in buffer])
        except BaseException as e:
            log(WARNING, e)
            # traceback.print_exc(file=sys.stdout)

    async def main(self):

        db_conn = await asyncpg.connect(host='localhost', user='sunqf', database='sunqf')

        async with db_conn.transaction():
            records = await db_conn.fetch("SELECT url from raw_html where type='%s'" % self.type)
            finished = set([r['url'] for r in records])

        print('finished len = %d' % len(finished))

        async for url, content in self.crawl(finished):
            await self.save(db_conn, [(url, content)])

    async def get_url_from_page(self, seeds: Set[str], article_selector: str, page_selector: str):
        pages = seeds.copy()
        finished = set()
        while len(pages) > 0:
            seed_url = pages.pop()
            seed_html = await self.get_html(seed_url)
            if seed_html:
                finished.add(seed_url)
                seed_html = BeautifulSoup(seed_html, 'html.parser')

                # get article href
                if article_selector:
                    for article in seed_html.select(article_selector):
                        article_href = article.attrs['href']
                        if not article_href.startswith('javascript'):
                            article_url = urljoin(seed_url, article_href)
                            yield article_url

                            finished.add(article_url)

                # get page href
                if page_selector:
                    for page in seed_html.select(page_selector):
                        page_href = page.attrs['href']
                        if not page_href.startswith('javascript'):
                            page_url = urljoin(seed_url, page_href)
                            if page_url not in finished and page_url.startswith(self.host):
                                pages.add(page_url)



class HttpCrawler(BaseCrawler):
    def __init__(self, max_worker, timeout, max_try_times, use_proxy=False, *args, **kwargs):
        self.max_worker = max_worker
        self.worker_sem = asyncio.Semaphore(max_worker)
        self.timeout = timeout
        self.max_try_times = max_try_times

        self.use_proxy = use_proxy

        self.client_session = ClientSession(headers=headers)
        super(HttpCrawler, self).__init__(*args, **kwargs)

    def __del__(self):
        self.client_session.close()

    async def get_html(self, url) -> Optional[str]:
        #if self.robot_parser and not self.robot_parser.can_fetch('*', url):
        #    log(WARNING, 'robots.txt disallow fetch %s' % url)
        #    return None

        if url_has_any_extension(url, extensions=IGNORED_EXTENSIONS):
            return None

        async with self.worker_sem:
            log(INFO, url)
            try_time = 0
            while self.max_try_times < 0 or try_time < self.max_try_times:
                try_time += 1
                try:
                    if self.use_proxy:
                        proxy = await fetch_proxy()
                        proxy_url = "http://{}".format(proxy)
                    async with self.client_session.get(url,
                                                       timeout=self.timeout,
                                                       proxy=proxy_url if self.use_proxy else None) as response:
                        if response.status == 200:
                            data = await response.read()
                            dammit = UnicodeDammit(data)
                            return dammit.unicode_markup
                        elif response.status == 404:
                            log(WARNING, '%s get 404' % url)
                            return None
                        elif response.status == 403:
                            log(WARNING, '%s get 403' % url)
                            return None
                        log(WARNING, '%s status %d' % (url, response.status))
                except Exception as e:
                    log(WARNING, e)
                    traceback.print_exc()
                    pass

        log(WARNING, 'Getting %s exceed %d times.' % (url, try_time))
        return None


class HeadlessCrawler(BaseCrawler):
    def __init__(self, max_worker: int, timeout: int, max_try_times: int, loop, *args, **kwargs):
        self.max_worker = max_worker
        self.timeout = timeout
        self.max_try_times = max_try_times
        self.loop = loop if loop else asyncio.get_event_loop()
        self.headless = Headless(loop, pool_size=max_worker)
        super(HeadlessCrawler, self).__init__(*args, **kwargs)

    def __del__(self):
        self.headless.close()

    async def get_html(self, url: str) -> Optional[str]:
        if self.robot_parser and not self.robot_parser.can_fetch('*', url):
            log(WARNING, 'robots.txt disallow fetch %s' % url)
            return None

        if url_has_any_extension(url, extensions=IGNORED_EXTENSIONS):
            return None

        if hasattr(self, 'content_locator'):
            return await self.headless.get(url, self.content_locator, self.timeout)
        else:
            return await self.headless.get(url, None, self.timeout)

