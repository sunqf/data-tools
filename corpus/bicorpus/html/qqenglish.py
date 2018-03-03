#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import asyncio
from logging import *

class QQEnglish(HttpCrawler):
    def __init__(self):
        super(QQEnglish, self).__init__(max_worker=10,
                                        timeout=10,
                                        max_try_times=10,
                                        type='qqenglish',
                                        host='http://www.qqenglish.com/')

        self.article_selector = '#list > div > ul > li > a[href]'
        self.page_selector = '#list > div > div > a[href]'

    async def get_urls(self):
        indexes = {'http://www.qqenglish.com/bn/', 'http://www.qqenglish.com/wsj/'}
        seeds = set()

        for index in indexes:
            index_html = await self.get_html(index)
            if index_html:
                for tag in BeautifulSoup(index_html, 'html.parser').select('#List_Title > span > a'):
                    seeds.add(urljoin(index, tag.attrs['href']))

        async for url in self.get_url_from_page(seeds, self.article_selector, self.page_selector):
            yield url


if __name__ == '__main__':
    getLogger().setLevel(INFO)
    loop = asyncio.get_event_loop()
    crawler = QQEnglish()
    loop.run_until_complete(crawler.main())
    loop.close()