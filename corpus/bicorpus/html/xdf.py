#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio

class XDF(HttpCrawler):
    def __init__(self):
        super(XDF, self).__init__(max_worker=10,
                                  timeout=10,
                                  max_try_times=10,
                                  type='xdf',
                                  host='http://yingyu.xdf.cn')

        self.seeds = {'http://yingyu.xdf.cn/list_4474_1.html',
                 'http://yingyu.xdf.cn/list_1799_1.html',
                 'http://yingyu.xdf.cn/list_1797_1.html',
                 'http://yingyu.xdf.cn/list_6128_1.html',
                 'http://yingyu.xdf.cn/list_6122_1.html'
                 'http://yingyu.xdf.cn/list_906_1.html',
                 'http://yingyu.xdf.cn/list_6116_1.html',
                 'http://yingyu.xdf.cn/list_6117_1.html',
                 'http://yingyu.xdf.cn/list_6118_1.html',
                 'http://yingyu.xdf.cn/list_6119_1.html'}

        self.article_selector = '#li_list > li > a[href]'
        self.page_selector = 'body > div.content > div.conL-box > div > div > ul > li > a[href]'

    async def get_urls(self):
        async for url in self.get_url_from_page(self.seeds, self.article_selector, self.page_selector):
            yield url


if __name__ == '__main__':
    xdf = XDF()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(xdf.main())
    loop.close()