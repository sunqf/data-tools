#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
from corpus.util.crawler import HttpCrawler
from bs4 import BeautifulSoup

class Eywedu(HttpCrawler):
    def __init__(self):
        super(Eywedu, self).__init__(max_worker=1,
                                     timeout=10,
                                     max_try_times=10,
                                     type='eywedu',
                                     host='http://en.eywedu.net/')

    async def get_urls(self):
        index = 'http://en.eywedu.net/indexs.htm'
        index_html = await self.get_html(index)
        if index_html:
            index_html = BeautifulSoup(index_html, 'html.parser')
            for tag in index_html.select('body > div:nth-child(3) > table:nth-child(2) > tbody > tr > td > table > tbody > tr:nth-child(2) ' \
                                         '> td > b > div > center > table > tbody > tr:nth-child(2) > td:nth-child(1) > div > big > font > a'):
                pass