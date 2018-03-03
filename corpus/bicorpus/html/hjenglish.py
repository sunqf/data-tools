#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import asyncio


class HJEnglish(HttpCrawler):
    def __init__(self):
        super(HJEnglish, self).__init__(max_worker=10,
                                        timeout=10,
                                        max_try_times=10,
                                        type='hjenglish',
                                        host='http://www.hjenglish.com/')


        self.seeds = {'http://www.hjenglish.com/new/fanyiyuedu/'}
        self.article_selector = 'body > div.pane.layout.pane-main > div > div.col.col-main-left > div.module.module-article-content > ul > li > a.big-link.title-article'
        self.page_selector = 'body > div.pane.layout.pane-main > div > div.col.col-main-left > div.module-rendered-pagination > ul > li > a[href]'

    async def get_urls(self):
        pages = self.seeds.copy()
        finished = set()
        while len(pages) > 0:
            seed = pages.pop()
            seed_html = await self.get_html(seed)
            if seed_html:
                finished.add(seed)
                seed_html = BeautifulSoup(seed_html, 'html.parser')

                # get article href
                total = 0
                if self.article_selector:
                    for article in seed_html.select(self.article_selector):
                        article_url = urljoin(seed, article.attrs['href'])
                        yield article_url
                        total += 1

                # get page href
                if total > 0 and self.page_selector:
                    for page in seed_html.select(self.page_selector):
                        page_url = urljoin(seed, page.attrs['href'])
                        if page_url not in finished:
                            pages.add(page_url)


if __name__ == '__main__':
    crawler = HJEnglish()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.main())
    loop.close()