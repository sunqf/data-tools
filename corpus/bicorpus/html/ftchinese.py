#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import re
import asyncio
from corpus.util.crawler import HttpCrawler
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# http://www.ftchinese.com/story/001075988/ce#adchannelID=1100 -> 001075988
ID = re.compile('/([0-9]{9})/')

class FTchinese(HttpCrawler):
    def __init__(self):
        super(FTchinese, self).__init__(max_worker=1,
                                        timeout=10,
                                        max_try_times=10,
                                        use_proxy=False,
                                        type='ftchinese',
                                        host='http://www.ftchinese.com/')

        self.id_extractor = re.compile('([0-9]{9})')

    async def get_urls(self):
        index = 'http://www.ftchinese.com/channel/ce.html'
        index_html = await self.get_html(index)
        if index_html:
            index_html = BeautifulSoup(index_html, 'html.parser')
            max_id = None
            href_template = None
            for tag in index_html.select('body > div.block-container.has-side.side-right > div > div.content-container > div > div.list-container > div > div > div > div > h2 > a[href]'):
                href = tag.attrs['href']
                id = int(self.id_extractor.findall(href)[0])
                if max_id is None or id > max_id:
                    max_id = id
                if href_template is None:
                    href_template = self.id_extractor.subn('%09d', href, 1)[0]

            for id in range(max_id, int('001005001'), -1):
                url = urljoin(index, href_template % id)
                yield url


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    crawler = FTchinese()
    loop.run_until_complete(crawler.main())
    loop.close()


