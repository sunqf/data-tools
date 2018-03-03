#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from corpus.util.headless import Headless
from corpus.util.crawler import HeadlessCrawler
import asyncio

class Iyuba(HeadlessCrawler):
    def __init__(self, loop):
        super(Iyuba, self).__init__(max_worker=10, timeout=10, max_try_times=10, loop=loop,
                                    type='iyuba', host='http://www.iyuba.com/')

        self.headless = Headless(loop, 10)

        self.content_locator = (By.CLASS_NAME, 'col-sm-12')

    def get_link(self, id, type):
        return 'http://www.iyuba.com/newdetail.jsp?type={}&id={}'.format(type, id)

    async def get_urls(self):
        index_locator = (By.CSS_SELECTOR, '#eNews > div > input[type="hidden"]')
        content_locator = (By.CLASS_NAME, 'col-sm-12')

        index = await self.headless.get(self.host, index_locator)
        if index:
            index = BeautifulSoup(index, 'html.parser')
        print(index)
        # news
        # #eNews > div:nth-child(1) > input[type="hidden"]
        head_new = index.select('#eNews > div > input[type="hidden"]')[0]
        max_id = int(head_new.attrs['value'])
        head_type = head_new.attrs['datatype']

        for id in range(max_id, 40253, -1):
            url = self.get_link(id, head_type)
            yield url

        # meida
        for tag in index.select('#audioList > div > input[type="hidden"]'):
            type = tag.attrs['datatype']
            max_id = int(tag.attrs['value'])
            for id in range(max_id, 1, -1):
                url = self.get_link(id, type)
                yield url

        tag = index.select('#videoList > div > input[type="hidden"]')[0]
        type = tag.attrs['datatype']
        max_id = int(tag.attrs['value'])
        for id in range(max_id, 1, -1):
            url = self.get_link(id, type)
            yield url


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    iyuba = Iyuba(loop)
    loop.run_until_complete(iyuba.main())
    loop.close()