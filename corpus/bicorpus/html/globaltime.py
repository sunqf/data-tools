#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

from corpus.util.crawler import HttpCrawler

class GlobalTime(HttpCrawler):

    def __init__(self):
        super(GlobalTime, self).__init__(max_worker=10, timeout=10, max_try_times=10,
                                         type='globaltime', host='http://language.globaltimes.cn/')


    async def get_urls(self):
