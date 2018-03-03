#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio

class I21st(HttpCrawler):
    def __init__(self):
        super(I21st, self).__init__(max_worker=10, timeout=10, max_try_times=10,
                                    type='i21st', host='http://www.i21st.cn/')

        self.seeds = {'http://www.i21st.cn/story/index_1.html'}
        self.article_selector = 'td.border1 > div > div.introduction.txt-12 > div.ft-family.ft-red > a[href]'
        self.page_selector = 'td.border1 > center > span > a[href]'

    async def get_urls(self):
        async for url in self.get_url_from_page(self.seeds, self.article_selector, self.page_selector):
            yield url

if __name__ == '__main__':
    crawler = I21st()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.main())
    loop.close()