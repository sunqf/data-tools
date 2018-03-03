#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio

class Cankaoxiaoxi(HttpCrawler):
    def __init__(self):
        super(Cankaoxiaoxi, self).__init__(max_worker=1, timeout=10, max_try_times=10,
                                           type='cankaoxiaoxi', host='http://www.cankaoxiaoxi.com/')

        self.seeds = {'http://www.cankaoxiaoxi.com/ym/sytd/'}
        self.article_selector = 'div.inner.mar-10 > ul > li > h4 > a[href]'
        self.page_selector = 'div.inner.mar-10 > div > ul > li > a[href]'


    async def get_urls(self):
        async for url in self.get_url_from_page(self.seeds, self.article_selector, self.page_selector):
            yield url


if __name__ == '__main__':
    crawler = Cankaoxiaoxi()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.main())
    loop.close()