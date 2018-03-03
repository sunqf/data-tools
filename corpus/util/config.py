#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

import asyncio
from aiohttp import ClientSession
import traceback
from urllib.parse import urljoin
from bs4 import UnicodeDammit

DSN = "dbname=sunqf user=sunqf"

proxy_get_url = 'http://127.0.0.1:5010/get/'


def proxy_delete_url(proxy):
    return "http://127.0.0.1:5010/delete/?proxy={}".format(proxy)


def get_url(keyword):
    return 'http://dict.youdao.com/jsonapi?q=lj:%s&doctype=json' % keyword


headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
}


async def fetch_proxy():
    while True:
        try:
            async with ClientSession() as proxy_session:
                async with proxy_session.get(proxy_get_url) as proxy_response:
                    if proxy_response.status == 200:
                        content = await proxy_response.text()
                        if len(content.split(':')) == 2:
                            return content
        except BaseException as e:
            print(e)


async def delete_proxy(proxy):
    async with ClientSession() as proxy_session:
        async with proxy_session.get(proxy_delete_url(proxy)) as proxy_response:
            pass

sem = asyncio.Semaphore(5)


async def get_html(url, timeout=10, max_try_times=10):
    async with sem:
        print(url)
        try_time = 0
        while max_try_times < 0 or try_time < max_try_times:
            try_time += 1
            try:
                async with ClientSession(headers=headers) as session:
                    async with session.get(url, timeout=timeout) as response:
                        if response.status == 200:
                            data = await response.read()
                            dammit = UnicodeDammit(data)
                            return dammit.unicode_markup
                        elif response.status == 404:
                            print('%s get 404' % url)
                            return None
                        elif response.status == 403:
                            print('%s get 403' % url)
                            return None
                        raise RuntimeError('raise for try again.')
            except UnicodeDecodeError as e:
                print(url, e)
                return None
            except Exception as e:
                print(url, e)
                traceback.print_exc()
                pass

    print('Getting %s exceed %d times.' % (url, try_time))
    return None


class Seed:
    def __init__(self, url, article_pattern, page_pattern):
        self.url = url
        self.article_pattern = article_pattern
        self.page_pattern = page_pattern

    def __hash__(self):
        return hash(self.url)

    def __eq__(self, other):
        return self.url == other.url
