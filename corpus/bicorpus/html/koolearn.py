#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio


class Koolearn(HttpCrawler):
    def __init__(self):
        super(Koolearn, self).__init__(max_worker=10,
                                       timeout=10,
                                       max_try_times=10,
                                       use_proxy=False,
                                       type='koolearn',
                                       host='http://english.koolearn.com/')

        self.seeds = {
            'http://english.koolearn.com/study/kouyu/',
            'http://english.koolearn.com/study/tingli/',
            'http://english.koolearn.com/study/meiju/',
            'http://english.koolearn.com/study/yingju/',
            'http://english.koolearn.com/study/lvyou/',
            'http://english.koolearn.com/study/mianshi/',
            'http://english.koolearn.com/study/zcyy/',
            'http://english.koolearn.com/study/rcshyy/',
            'http://english.koolearn.com/study/kids/',
            'http://english.koolearn.com/study/song/',
            'http://english.koolearn.com/study/syuyuedu/synews/',
            'http://english.koolearn.com/study/syuyuedu/symw/'
        }

        self.article_selector = '#cr > div.w685.maincn.fl > div.list01 > ul > li > h3 > a[href]'
        self.page_selector = '#page > a[href]'

    async def get_urls(self):
        async for url in self.get_url_from_page(self.seeds, self.article_selector, self.page_selector):
            yield url


if __name__ == '__main__':

    crawler = Koolearn()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.main())
    loop.close()