#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from corpus.util.crawler import HttpCrawler
import asyncio

class Iciba(HttpCrawler):
    def __init__(self):
        super(Iciba, self).__init__(max_worker=10, timeout=10, max_try_times=10,
                                    type='iciba', host='http://news.iciba.com/')

        self.seeds = {
            'http://news.iciba.com/study/syxw/',
            'http://news.iciba.com/salon/dianying/',
            'http://news.iciba.com/study/meiwen/',
            'http://news.iciba.com/salon/meiju/',
            'http://news.iciba.com/salon/shishang/',
            'http://news.iciba.com/salon/lvyou/',
            'http://news.iciba.com/study/dianjin/',
            'http://news.iciba.com/study/richang/',
        }

        self.article_selector = 'body > div.big_back > div > div.content > div > div > li > a[href]'

        self.page_selector = 'body > div.big_back > div > div.content > div > div.page > a[href]'

    async def get_urls(self):
        async for url in self.get_url_from_page(self.seeds, self.article_selector, self.page_selector):
            yield url


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    crawler = Iciba()
    loop.run_until_complete(crawler.main())
    loop.close()