#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
import argparse
import json
from typing import List, Set
import asyncpg
from tqdm import tqdm
from bs4 import BeautifulSoup, Tag
from corpus.util.config import *
from corpus.bicorpus import db
from logging import *

getLogger().setLevel(INFO)

# https://www.bing.com/dict/service?q=cribriform&offset=0&dtype=sen&&FORM=BDVSP6&mkt=zh-cn
def get_url(word, offset=0):
    return 'https://www.bing.com/dict/service?q={}&offset={}&dtype=sen&&FORM=BDVSP6&mkt=zh-cn'.format(word, offset)


def extract_pair(html: BeautifulSoup):
    en_tags = html.find_all(class_='sen_en')
    cn_tags = html.find_all(class_='sen_cn')
    ens = [tag.text for tag in en_tags]
    cns = [tag.text for tag in cn_tags]
    sources = [tag.attrs['href'] for tag in html.find_all(rel="external nofollow")]
    words = set([a.text.strip() for tag in en_tags for a in tag.select('a[href]')]
                + [a.text.strip() for tag in cn_tags for a in tag.select('a[href]')])

    return [{'en': en, 'cn': cn, 'source': source} for en, cn, source in zip(ens, cns, sources)], words

class BingCrawler:
    HOST = 'https://www.bing.com'
    def __init__(self, keywords: Set[str], finished: Set[str], db_pool, client_session, semaphore):

        self.keywords = keywords
        self.finished = finished
        self.db_pool = db_pool
        self.client_session = client_session
        self.semaphore = semaphore

        self.max_try_times = 10
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

    async def fetch_by_word(self, keyword: str):
        pairs = []
        words = set()
        offset = 0
        while True:
            url = get_url(keyword, offset)
            html = await self.get_html(url)
            if html:
                html = BeautifulSoup(html, 'html.parser')
                _pairs, _words = extract_pair(html)
                pairs += _pairs
                words.update(_words)
                if html.find(class_='b_pag') is None:
                    break
            else:
                break
            offset += 10

        return keyword, pairs, words

    async def save_keyword(self, keyword):
        try:
            async with self.db_pool.acquire() as db_conn:
                async with db_conn.transaction():
                    await db_conn.execute("INSERT INTO bing (keyword) VALUES ($1)", keyword)
        except BaseException as e:
            print(e)
            #traceback.print_exc(file=sys.stdout)

    async def save(self, pairs):
        try:
            async with self.db_pool.acquire() as db_conn:
                async with db_conn.transaction():
                    await db_conn.executemany("INSERT INTO ch2en (ch, en, url, source) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                                              [(pair['cn'], pair['en'], pair['source'], 'bing') for pair in pairs])
        except BaseException as e:
            print(e)
            #traceback.print_exc(file=sys.stdout)

    async def bound(self, func, *args, **kwargs):
        async with self.semaphore:
            return await func(*args, **kwargs)

    async def main(self,):
        while len(self.keywords) > 0:
            batch = set()
            futures = []
            while len(futures) < 10000 and len(self.keywords) > 0:
                keyword = self.keywords.pop()
                batch.add(keyword)
                futures.append(self.bound(self.fetch_by_word, keyword))

            for future in tqdm(asyncio.as_completed(futures), total=len(futures)):
                keyword, pairs, new_words = await future

                if len(pairs) > 0:
                    await self.save(pairs)
                await self.save_keyword(keyword)

                self.finished.add(keyword)
                for new_word in new_words:
                    if new_word not in batch and new_word not in self.finished:
                        self.keywords.add(new_word)


    @classmethod
    async def create(cls, loop, keywords, max_worker=10):
        db_pool = await db.connect_pool()

        async with db_pool.acquire() as db_conn:
            async with db_conn.transaction():
                records = await db_conn.fetch('SELECT keyword from bing')
                finished = set([r['keyword'] for r in records])

        keywords = set([word for word in keywords if word not in finished])

        client_session = ClientSession(headers=headers, loop=loop)

        return BingCrawler(keywords, finished, db_pool, client_session, semaphore = asyncio.Semaphore(max_worker))


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--dict', type=str, help='dict path')

args = arg_parser.parse_args()

words = set()
with open(args.dict) as file:
    for line in file:
        items = line.strip().split('\t')
        if len(items[0]) > 0:
            words.add(items[0])


loop = asyncio.get_event_loop()

bing = loop.run_until_complete(BingCrawler.create(loop, words, 10))
loop.run_until_complete(bing.main())
loop.close()