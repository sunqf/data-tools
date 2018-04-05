#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import asyncio

class _51VOA(HttpCrawler):

    def __init__(self):
        super(_51VOA, self).__init__(max_worker=10, timeout=10, max_try_times=10,
                                     type='51voa', host='http://www.51voa.com/')

        self.article_selector = '#list > ul > li > a[href]'
        self.page_selector = 'a[href]'
        self.multi_page_selector = '#EnPage'

    async def get_urls(self):
        async for url in self.get_url_from_page(set([self.host]), self.article_selector, self.page_selector):
            yield url

    def select_multi_page(self, url, content: BeautifulSoup):
        for a in content.select(self.multi_page_selector):
            href = a.attrs['href']
            if not href.startswith('javascript'):
                new_url = urljoin(url, href)
                yield new_url


if __name__ == '__main__':
    crawler = _51VOA()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.main())
    loop.close()
