#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio

class TingClass(HttpCrawler):
    def __init__(self):
        super(TingClass, self).__init__(max_worker=10, timeout=10, max_try_times=10,
                                        type='tingclass', host='http://www.tingclass.net/')

        self.seeds = {
            'http://www.tingclass.net/list-138-1.html',
            'http://www.tingclass.net/list-459-1.html',
            'http://www.tingclass.net/list-500-1.html',
            'http://www.tingclass.net/list-506-1.html',
            'http://www.tingclass.net/list-567-1.html',
            'http://www.tingclass.net/list-568-1.html',
            'http://www.tingclass.net/list-7809-1.html'
        }
        self.article_selector = 'body > div.main-con.rel.thousand > div.main-lft-con.thousand-w685 > dl > dt > a[href]'
        self.page_selector = '#pages > ul > li > a[href]'


        self.seeds2 = {
            'http://www.tingclass.net/list-566-1.html',
            'http://www.tingclass.net/list-569-1.html'
        }
        self.article_selector2 = 'body > div.main-con.rel.thousand > div.main-lft-con.thousand-w685 > div.sigle-tag.fl.thousand-w685.thousand-mr0 > div.tab-con > ul > li > a[href]'
        self.page_selector2 = '#uyan_frame01 > ul > li > a[href]'

    async def get_urls(self):
        async for url in self.get_url_from_page(self.seeds, self.article_selector, self.page_selector):
            yield url

        async for url in self.get_url_from_page(self.seeds2, self.article_selector2, self.page_selector2):
            yield url


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    crawler = TingClass()
    loop.run_until_complete(crawler.main())
    loop.close()