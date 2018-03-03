#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import argparse
import json
from typing import List, Set, Dict
import asyncpg
from tqdm import tqdm
from bs4 import BeautifulSoup, Tag
from corpus.util.config import *
from logging import *
from .urls import *

getLogger().setLevel(INFO)

# https://www.bing.com/dict/service?q=cribriform&offset=0&dtype=sen&&FORM=BDVSP6&mkt=zh-cn


def extract_pair(html: BeautifulSoup):
    return [{'text': tag.text, 'url': urljoin(Dictall.HOST, tag.attrs['href'])}for tag in html.select('#catelist > a')]

class Dictall:
    HOST = 'http://www.dictall.com'
    def __init__(self, urls: Dict[str, str], db_conn, client_session, semaphore):

        self.urls = urls
        self.db_conn = db_conn
        self.client_session = client_session
        self.semaphore = semaphore

        self.max_try_times = -1
        self.use_proxy = False
        self.timeout = 10

    def __del__(self):
        self.client_session.close()

    async def get_html(self, url):
        # if self.robot_parser and not self.robot_parser.can_fetch('*', url):
        #    log(WARNING, 'robots.txt disallow fetch %s' % url)
        #    return None

        async with self.semaphore:
            log(INFO, url)
            try_time = 0
            while self.max_try_times < 0 or try_time < self.max_try_times:
                try_time += 1
                try:
                    if self.use_proxy:
                        proxy = await fetch_proxy()
                        proxy_url = "http://{}".format(proxy)
                    async with self.client_session.get(url,
                                                       timeout=self.timeout,
                                                       proxy=proxy_url if self.use_proxy else None) as response:
                        if response.status == 200:
                            data = await response.read()
                            dammit = UnicodeDammit(data)
                            return dammit.unicode_markup
                        elif response.status == 404:
                            log(WARNING, '%s get 404' % url)
                            return None
                        elif response.status == 403:
                            log(WARNING, '%s get 403' % url)
                            return None
                        log(WARNING, '%s status %d' % (url, response.status))
                except Exception as e:
                    log(WARNING, e)
                    traceback.print_exc()
                    pass

        log(WARNING, 'Getting %s exceed %d times.' % (url, try_time))
        return None

    async def fetch(self, category: str, url: str):
        pairs = []
        while True:
            html = await self.get_html(url)
            if html:
                html = BeautifulSoup(html, 'html.parser')
                _pairs = extract_pair(html)
                pairs += _pairs
                next_tag = html.find(title="下一页")
                if next_tag:
                    url = urljoin(self.HOST, next_tag.attrs['href'])
                else:
                    break
            else:
                break

        return category, url, pairs

    async def save(self, keyword: str, last_url: str, data: str):
        try:
            async with self.db_conn.transaction():
                await self.db_conn.execute("INSERT INTO dictall(category, url, data) VALUES ($1, $2, $3)",
                                           keyword, last_url, data)
        except BaseException as e:
            log(WARNING, e)
            #traceback.print_exc(file=sys.stdout)

    async def bound(self, func, *args, **kwargs):
        async with self.semaphore:
            return await func(*args, **kwargs)

    async def main(self,):

        for category, url in tqdm(self.urls.items(), total=len(self.urls)):
            #if category not in self.finished:
            _, last_url, pairs = await self.fetch(category, url)

            await self.save(category, last_url, json.dumps(pairs, ensure_ascii=False))


    @classmethod
    async def create(cls, loop, categories, max_worker=10):
        db_conn = await asyncpg.connect(host='localhost', user='sunqf', database='sunqf', loop=loop)

        async with db_conn.transaction():
            records = await db_conn.fetch('SELECT category from dictall')
            finished = set([r['category'] for r in records])

        client_session = ClientSession(headers=headers, loop=loop)

        return Dictall(categories, db_conn, client_session, semaphore = asyncio.Semaphore(max_worker))


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--type', type=str, help='[word, phrase, industry]')

    args = arg_parser.parse_args()

    loop = asyncio.get_event_loop()

    mapping = {
        'word': word_list,
        'phrase': phrase_list,
        'industry': industry_list
    }

    if args.type in mapping:
        dictall = loop.run_until_complete(Dictall.create(loop, mapping[args.type], 10))
        loop.run_until_complete(dictall.main())
        dictall.close()
        loop.close()







