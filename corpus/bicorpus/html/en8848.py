#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class En8848(HttpCrawler):
    def __init__(self):
        super(En8848, self).__init__(max_worker=10, timeout=10, max_try_times=10,
                                     type='en8848', host='http://www.en8848.com.cn/')

        self.seeds = {
            'http://www.en8848.com.cn/read/bi/'
        }
        self.article_selector = 'body > div > div.ch_warp_left > div.ch_content > div > div.ch_li_right > h4 > a[href]'
        self.page_selector = 'body > div > div.ch_warp_left > div.ch_content > div.ch_pagebar > a[href]'

    async def get_urls(self):
        async for url in self.get_url_from_page(self.seeds, self.article_selector, self.page_selector):
            yield url

        seed_url = list(self.seeds)[0]
        html = await self.get_html(seed_url)
        if html:
            html = BeautifulSoup(html, 'html.parser')
            children = set()
            for t in html.select('body > div > div.ch_warp_left > ul > li > a[href]'):
                t = urljoin(seed_url, t.attrs['href'])
                children.add(t)

            async for url in self.get_url_from_page(children,
                                                    'body > div > div.ch_warp_left > div.ch_content > div > div > a[href]',
                                                    'body > div > div.ch_warp_left > div.ch_content > div.ch_pagebar > a[href]'
                                                    ):
                yield url


if __name__ == '__main__':
    crawler = En8848()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.main())
    loop.close()