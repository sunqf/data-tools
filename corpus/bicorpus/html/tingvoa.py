#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio


class TingVOA(HttpCrawler):

    def __init__(self):
        super(TingVOA, self).__init__(max_worker=10, timeout=10, max_try_times=10,
                                      type='tingvoa', host='http://www.tingvoa.com/')
        self.article_selector = '#mainleftlist > div.newslist > dl > dt > span > a[href]'
        self.page_selector = 'a[href]'

    # 全网抓取
    async def get_urls(self):
        async for url in self.get_url_from_page(set([self.host]), self.article_selector, self.page_selector):
            yield url


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    crawler = TingVOA()
    loop.run_until_complete(crawler.main())
    loop.close()