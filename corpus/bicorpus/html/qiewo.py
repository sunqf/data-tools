#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from corpus.util.crawler import HttpCrawler
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import asyncio

class Qiewo(HttpCrawler):
    def __init__(self):
        super(Qiewo, self).__init__(max_worker=10, timeout=10, max_try_times=10,
                                    type='qiewo', host='http://www.qiewo.com/')

        self.seeds = {'http://www.qiewo.com/ting/broadcast/Bilingual-News/'}
        self.article_selector = 'div.box_space > div.stock > div.box_L01 > div.zliebiao > ul > li > a[href]'
        self.page_selector = 'div.box_space > div.stock > div.box_L01 > div.epages > a[href]'

    async def get_urls(self):
        ting = 'http://www.qiewo.com/ting/'
        selector = 'div > div.stock > div.box_L01 > div > ul > li > a'
        pages = set()

        ting_html = await self.get_html(ting)
        if ting_html:
            index_html = BeautifulSoup(ting_html, 'html.parser')
            for page in index_html.select(selector):
                page = urljoin(ting, page.attrs['href'])
                pages.add(page)

        async for url in self.get_url_from_page(pages.union(self.seeds), self.article_selector, self.page_selector):
            yield url


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    crawler = Qiewo()
    loop.run_until_complete(crawler.main())
    loop.close()