#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
import asyncio

class _24EN(HttpCrawler):
    def __init__(self):
        super(_24EN, self).__init__(max_worker=10, timeout=10, max_try_times=10,
                                    type='24en', host='http://www.24en.com/')

        self.seeds = {
            'http://www.24en.com/fun/bilingual/',
            'http://www.24en.com/fun/soul/',
            'http://www.24en.com/fun/poesy/',
            'http://www.24en.com/fun/chefdoeuvre/',
            'http://www.24en.com/fun/wisdom/',
            'http://www.24en.com/fun/interest/',

            'http://www.24en.com/read/news/life/',
            'http://www.24en.com/read/news/politics/',
            'http://www.24en.com/read/news/culture-education/',
            'http://www.24en.com/read/news/finance-economics/',
            'http://www.24en.com/read/news/science-technology/',
            'http://www.24en.com/read/news/sports/',
            'http://www.24en.com/cnn/'

            'http://www.24en.com/read/al/',
            'http://www.24en.com/read/meiwen/',
            'http://www.24en.com/read/speech/',
            'http://www.24en.com/read/digest/',
            'http://www.24en.com/read/story/',
            'http://www.24en.com/read/prose/',
            'http://www.24en.com/read/culture/',
            'http://www.24en.com/read/joke/',
            'http://www.24en.com/read/poetry/',
            'http://www.24en.com/read/myjj/',
            'http://www.24en.com/read/tourism/',

        }
        self.article_selector = 'body > div.box_main > div.container.margin-top-20.clearfix > div.box_left > div.part > div > div > a[href]'
        self.page_selector = 'body > div.box_main > div.container.margin-top-20.clearfix > div.box_left > div.pages > div.pagination > a[href]'


    # TODO: 小说http://www.24en.com/novel/

    async def get_urls(self):
        async for url in self.get_url_from_page(self.seeds, self.article_selector, self.page_selector):
            yield url


if __name__ == '__main__':
    crawler = _24EN()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.main())
    loop.close()