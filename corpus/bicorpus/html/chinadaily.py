#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio

class ChinaDaily(HttpCrawler):
    def __init__(self):
        super(ChinaDaily, self).__init__(max_worker=10,
                                         timeout=10,
                                         max_try_times=10,
                                         type='chinadaily',
                                         host='http://language.chinadaily.com.cn/'
                                         )

        self.article_selector = 'body > div.all > div.all-left > div > div > div > h3 > a[href]'
        self.page_selector = 'a[href]'


    async def get_urls(self):
        async for url in self.get_url_from_page(set([self.host]), self.article_selector, self.page_selector):
            yield url


if __name__ == '__main__':
    chinadaily = ChinaDaily()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(chinadaily.main())
    loop.close()